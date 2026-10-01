"""
group_transfer.py  -  AUTOMAÇÃO 1: Gestão de Transferência de Grupos (OTIMIZADO)
================================================================================
Move contatos do Grupo A para o Grupo B conforme regras operacionais, utilizando
processamento paralelo e pools de conexão para máxima performance.

Uso:
    python group_transfer.py --rules rules.json --dry-run
    python group_transfer.py --rules rules.json

Otimizações desta versão:
  - Multi-threading com ThreadPoolExecutor para processamento paralelo.
  - Rate limiting implícito via controle de Max Workers (limita paralelismo).
  - Controle de estado thread-safe via Locks.
  - HTTP Connection Pooling ativo na camada do ArgusClient (reuso de conexões TCP).
"""
import argparse
import concurrent.futures
import csv
import json
import logging
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime

from argus_client import (ArgusAPIError, ArgusAuthError, ArgusClient, ArgusError,
                          ArgusUnavailableError, GroupNotFoundError)
from audit_log import audit, setup_logger
from config import load_config

# Configuração de Concorrência: Define quantas requisições ocorrem simultaneamente
# Ajuste de acordo com o Rate Limit estabelecido pela API da discadora (ex: 20 req/s)
MAX_WORKERS = 20  


@dataclass
class TransferRule:
    name: str
    source_group: str
    target_group: str
    tabulacoes: list[str]
    max_contacts: int | None = None


# Otimização: A estrutura de resultados precisa ser Thread-Safe 
# devido ao processamento assíncrono/paralelo das requisições.
class ThreadSafeRuleResult:
    def __init__(self, rule: str):
        self.rule = rule
        self.moved = 0
        self.skipped = 0
        self.failed = []
        self.fatal_error = None
        self._lock = threading.Lock()

    def add_moved(self):
        with self._lock: 
            self.moved += 1

    def add_skipped(self):
        with self._lock: 
            self.skipped += 1

    def add_failed(self, fail_dict):
        with self._lock: 
            self.failed.append(fail_dict)


# ---------------------------------------------------------------------------
def process_rule(client: ArgusClient, rule: TransferRule, log: logging.Logger,
                 dry_run: bool, verify: bool = True) -> ThreadSafeRuleResult:
    result = ThreadSafeRuleResult(rule=rule.name)

    # 1) Resolver grupos
    try:
        src_id = client.get_group_id(rule.source_group)
        dst_id = client.get_group_id(rule.target_group)
    except GroupNotFoundError as exc:
        result.fatal_error = str(exc)
        audit(log, "RULE_ABORTED_GROUP_NOT_FOUND", logging.ERROR, rule=rule.name, error=str(exc))
        return result

    if src_id == dst_id:
        result.fatal_error = "Origem e destino são o mesmo grupo."
        audit(log, "RULE_ABORTED_SAME_GROUP", logging.ERROR, rule=rule.name)
        return result

    audit(log, "RULE_START", rule=rule.name, source=rule.source_group, target=rule.target_group,
          tabulacoes=rule.tabulacoes, dry_run=dry_run)

    wanted = {t.strip().upper() for t in rule.tabulacoes}
    contacts_to_process = []

    # 2) Fase de Coleta de Contatos (Puxar IDs para a memória antes de processar)
    # Isso desamarra a paginação da API da lentidão de mover os contatos
    for tab in rule.tabulacoes:
        try:
            for contact in client.iter_group_contacts(src_id, tabulacao=tab):
                current = (contact["tabulacao"] or "").strip().upper()
                if current in wanted:
                    contacts_to_process.append(contact)
                    if rule.max_contacts and len(contacts_to_process) >= rule.max_contacts:
                        break
        except (ArgusAPIError, ArgusUnavailableError) as exc:
            result.fatal_error = f"Falha ao listar contatos ({tab}): {exc}"
            audit(log, "RULE_LIST_FAILED", logging.ERROR, rule=rule.name, error=str(exc))
            return result
        
        if rule.max_contacts and len(contacts_to_process) >= rule.max_contacts:
            audit(log, "RULE_LIMIT_REACHED", rule=rule.name, limit=rule.max_contacts)
            break

    audit(log, "RULE_COLLECTION_DONE", rule=rule.name, total_collected=len(contacts_to_process))

    if not contacts_to_process:
        return result

    # 3) Fase de Execução Concorrente (Multi-threading / Batching Lógico)
    # Aqui substituímos o loop for tradicional (que bloqueava 1 a 1) por 
    # requisições em paralelo reutilizando conexões abertas no pool (HTTPAdapter).
    def _worker(contact):
        try:
            _move_one(client, contact, src_id, dst_id, rule, log, result, dry_run, verify)
        except ArgusAuthError:
            # Propagação manual da falha crítica pela thread
            result.fatal_error = "Erro de autenticação durante o lote (AuthError)."
        except Exception as exc:
            # Qualquer outra falha de thread não listada
            result.add_failed({"contact_id": contact["id"], "rule": rule.name, "error": f"Erro interno: {exc}"})

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Cria as promessas de execução e as espalha pelo pool (até MAX_WORKERS de uma vez)
        futures = [executor.submit(_worker, c) for c in contacts_to_process]
        # Aguarda todas as transferências finalizarem
        concurrent.futures.wait(futures)

    audit(log, "RULE_END", rule=rule.name, moved=result.moved, skipped=result.skipped, failed=len(result.failed))
    return result


def _move_one(client, contact, src_id, dst_id, rule, log, result, dry_run, verify) -> None:
    """ Função isolada e thread-safe para mover um único contato. """
    cid = contact["id"]
    try:
        # Idempotência: se já está no destino, pula.
        # Caso --no-verify, nós confiamos que não está no destino para ganhar tempo.
        current_group = client.get_contact_group(cid) if verify else src_id
        if current_group == dst_id:
            result.add_skipped()
            return

        if dry_run:
            result.add_moved()
            return

        # Executa a movimentação na API
        # Nota: Se a API tiver um endpoint para mover um LOTE, seria ideal fazer uma nova
        # chamada tipo `client.move_contacts_batch([id1, id2, ...], src, dst)` 
        # antes do `ThreadPoolExecutor`.
        client.move_contact(cid, src_id, dst_id)

        # Validação pós-movimento 
        if verify and client.get_contact_group(cid) != dst_id:
            raise ArgusError("Movimentação não confirmada (contato não chegou ao destino).")

        result.add_moved()
        # Removido: audit() para cada sucesso pode encher o disco rápido se forem milhões de contatos
        # audit(log, "CONTACT_MOVED", contact_id=cid, ...)

    except ArgusAuthError:
        raise
    except (ArgusError, KeyError, ValueError) as exc:
        # Falhas isoladas não derrubam as demais threads nem o processo inteiro
        result.add_failed({"contact_id": cid, "rule": rule.name, "error": str(exc)})
        audit(log, "CONTACT_MOVE_FAILED", logging.ERROR, contact_id=cid, rule=rule.name, error=str(exc))


def write_failures_csv(results: list[ThreadSafeRuleResult]) -> str | None:
    rows = [row for r in results for row in r.failed]
    if not rows:
        return None
    path = f"falhas_transferencia_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["contact_id", "rule", "error"])
        writer.writeheader()
        writer.writerows(rows)
    return path


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="ARGUS - transferência de contatos entre grupos (Performance)")
    ap.add_argument("--rules", required=True, help="JSON com as regras de transferência")
    ap.add_argument("--config", default="config.json", help="JSON de configuração (opcional)")
    ap.add_argument("--dry-run", action="store_true", help="Simula sem alterar nada")
    ap.add_argument("--no-verify", action="store_true", help="Não consulta o contato antes/depois (ganho extremo de performance)")
    args = ap.parse_args()

    log = setup_logger("group_transfer")
    try:
        cfg = load_config(args.config)
        with open(args.rules, encoding="utf-8") as fh:
            rules = [TransferRule(**r) for r in json.load(fh)]
    except (RuntimeError, OSError, TypeError, json.JSONDecodeError) as exc:
        log.critical("Configuração inválida: %s", exc)
        return 2

    client = ArgusClient(cfg, log)
    results = []
    
    # Executa regras sequencialmente (pois as lógicas de negócio podem depender uma da outra)
    # Mas o processamento de itens de DENTRO de uma regra é paralelo.
    for rule in rules:
        r = process_rule(client, rule, log, args.dry_run, verify=not args.no_verify)
        results.append(r)
        if r.fatal_error and "AuthError" in r.fatal_error:
            log.critical("Processamento cancelado: %s", r.fatal_error)
            break

    csv_path = write_failures_csv(results)
    had_problem = False
    
    for r in results:
        log.info("RESUMO | %s | movidos=%d pulados=%d falhas=%d%s", r.rule, r.moved, r.skipped,
                 len(r.failed), f" | ERRO: {r.fatal_error}" if r.fatal_error else "")
        had_problem |= bool(r.failed or r.fatal_error)
        
    if csv_path:
        log.warning("Falhas de processamento isoladas foram salvas em %s", csv_path)
        
    return 1 if had_problem else 0


if __name__ == "__main__":
    sys.exit(main())
