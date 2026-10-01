"""
group_transfer.py  -  AUTOMAÇÃO 1: Gestão de Transferência de Grupos
====================================================================
Move contatos do Grupo A para o Grupo B conforme regras operacionais
(ex.: contatos com tabulação "SEM CONTATO" saem da "Carteira Nova" e vão para "Retentativa").

Uso:
    python group_transfer.py --rules rules.json --dry-run     # só simula e loga
    python group_transfer.py --rules rules.json               # executa de verdade

Exemplo de rules.json:
[
  {
    "name": "Sem contato -> Retentativa",
    "source_group": "Carteira Nova",
    "target_group": "Retentativa",
    "tabulacoes": ["SEM CONTATO", "CAIXA POSTAL"],
    "max_contacts": 5000
  }
]

Fluxo de cada regra:
  1. resolve nome -> ID dos grupos (falha cedo se origem/destino não existir);
  2. varre os contatos da origem filtrando pela(s) tabulação(ões);
  3. move cada contato (idempotente: se já está no destino, apenas pula);
  4. confirma a movimentação consultando o contato;
  5. falhas individuais NÃO interrompem o lote; vão para um CSV de reprocessamento.

Códigos de saída: 0 = tudo ok | 1 = houve falhas parciais | 2 = erro fatal (auth, config)
"""
import argparse
import csv
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime

from argus_client import (ArgusAPIError, ArgusAuthError, ArgusClient, ArgusError,
                          ArgusUnavailableError, GroupNotFoundError)
from audit_log import audit, setup_logger
from config import load_config


@dataclass
class TransferRule:
    name: str
    source_group: str
    target_group: str
    tabulacoes: list[str]
    max_contacts: int | None = None


@dataclass
class RuleResult:
    rule: str
    moved: int = 0
    skipped: int = 0
    failed: list[dict] = field(default_factory=list)
    fatal_error: str | None = None


# ---------------------------------------------------------------------------
def process_rule(client: ArgusClient, rule: TransferRule, log: logging.Logger,
                 dry_run: bool, verify: bool = True) -> RuleResult:
    result = RuleResult(rule=rule.name)

    # 1) Resolver grupos (falha cedo se origem/destino não existir)
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
    processed = 0

    # 2) Varre os contatos (uma passada por tabulação, usando filtro server-side quando existir)
    for tab in rule.tabulacoes:
        try:
            contacts = client.iter_group_contacts(src_id, tabulacao=tab)
            for contact in contacts:
                # Segurança extra: confere a tabulação do lado do cliente
                current = (contact["tabulacao"] or "").strip().upper()
                if current not in wanted:
                    continue
                if rule.max_contacts and processed >= rule.max_contacts:
                    audit(log, "RULE_LIMIT_REACHED", rule=rule.name, limit=rule.max_contacts)
                    return result
                processed += 1
                _move_one(client, contact, src_id, dst_id, rule, log, result, dry_run, verify)
        except ArgusAuthError:
            raise                                   # erro fatal: propaga para o main
        except (ArgusAPIError, ArgusUnavailableError) as exc:
            result.fatal_error = f"Falha ao listar contatos ({tab}): {exc}"
            audit(log, "RULE_LIST_FAILED", logging.ERROR, rule=rule.name, tabulacao=tab, error=str(exc))
            return result

    audit(log, "RULE_END", rule=rule.name, moved=result.moved, skipped=result.skipped, failed=len(result.failed))
    return result


def _move_one(client, contact, src_id, dst_id, rule, log, result, dry_run, verify) -> None:
    cid = contact["id"]
    try:
        # Idempotência: se já está no destino, não repete a operação
        current_group = client.get_contact_group(cid) if verify else src_id
        if current_group == dst_id:
            result.skipped += 1
            audit(log, "CONTACT_SKIPPED_ALREADY_IN_TARGET", contact_id=cid, rule=rule.name)
            return

        if dry_run:
            result.moved += 1
            audit(log, "CONTACT_WOULD_MOVE", contact_id=cid, tabulacao=contact["tabulacao"], rule=rule.name)
            return

        client.move_contact(cid, src_id, dst_id)

        # Confirma que o contato realmente está no destino
        if verify and client.get_contact_group(cid) != dst_id:
            raise ArgusError("Movimentação não confirmada (contato não está no grupo destino).")

        result.moved += 1
        audit(log, "CONTACT_MOVED", contact_id=cid, tabulacao=contact["tabulacao"],
              source=rule.source_group, target=rule.target_group, rule=rule.name)

    except ArgusAuthError:
        raise
    except (ArgusError, KeyError, ValueError) as exc:
        result.failed.append({"contact_id": cid, "rule": rule.name, "error": str(exc)})
        audit(log, "CONTACT_MOVE_FAILED", logging.ERROR, contact_id=cid, rule=rule.name, error=str(exc))


def write_failures_csv(results: list[RuleResult]) -> str | None:
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
    ap = argparse.ArgumentParser(description="ARGUS - transferência de contatos entre grupos")
    ap.add_argument("--rules", required=True, help="JSON com as regras de transferência")
    ap.add_argument("--config", default="config.json", help="JSON de configuração (opcional)")
    ap.add_argument("--dry-run", action="store_true", help="Simula sem alterar nada")
    ap.add_argument("--no-verify", action="store_true", help="Não consulta o contato antes/depois (mais rápido)")
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
    results: list[RuleResult] = []
    try:
        for rule in rules:
            results.append(process_rule(client, rule, log, args.dry_run, verify=not args.no_verify))
    except ArgusAuthError as exc:
        log.critical("Erro de autenticação: %s", exc)
        return 2

    csv_path = write_failures_csv(results)
    had_problem = False
    for r in results:
        log.info("RESUMO | %s | movidos=%d pulados=%d falhas=%d%s", r.rule, r.moved, r.skipped,
                 len(r.failed), f" | ERRO: {r.fatal_error}" if r.fatal_error else "")
        had_problem |= bool(r.failed or r.fatal_error)
    if csv_path:
        log.warning("Falhas salvas para reprocessamento em %s", csv_path)
    return 1 if had_problem else 0


if __name__ == "__main__":
    sys.exit(main())
