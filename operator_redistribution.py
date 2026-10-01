"""
operator_redistribution.py
==========================
Script para automatizar a redistribuição de operadores de um grupo genérico 
(ex: "receptivo - ura") de volta para suas equipes operacionais de origem.

Regras:
  1. Carrega um mapeamento de Operador -> Grupo Destino.
  2. Varre o grupo origem listando todos os operadores presentes.
  3. Compara com o mapeamento e dispara a transferência concorrente.
  
Uso:
  python operator_redistribution.py --map operator_map.json --source "receptivo - ura"
"""

import argparse
import concurrent.futures
import json
import logging
import sys

from argus_client import ArgusAuthError, ArgusClient, ArgusError, GroupNotFoundError
from audit_log import audit, setup_logger
from config import load_config

# Ajuste conforme limite da API para não causar rate limit ao mover operadores
MAX_WORKERS = 10


def load_mapping(filepath: str, log: logging.Logger) -> dict[str, str]:
    """
    Carrega o dicionário de mapeamento Operador -> Grupo
    Formato esperado do JSON:
    {
       "login_joao": "Equipe_Vendas_A",
       "login_maria": "Equipe_Suporte_B"
    }
    """
    try:
        with open(filepath, encoding="utf-8") as fh:
            mapping = json.load(fh)
            log.info(f"Mapeamento carregado com sucesso: {len(mapping)} operadores mapeados.")
            return mapping
    except (OSError, json.JSONDecodeError) as exc:
        log.critical(f"Falha ao carregar o arquivo de mapeamento '{filepath}': {exc}")
        sys.exit(2)


def process_redistribution(client: ArgusClient, source_group_name: str, mapping: dict[str, str], 
                           log: logging.Logger, dry_run: bool):
    """
    Lê os operadores atuais da origem, cruza com o mapeamento e redistribui.
    """
    try:
        source_id = client.get_group_id(source_group_name)
    except GroupNotFoundError as exc:
        log.critical(f"Grupo de origem não encontrado: {exc}")
        return False

    audit(log, "REDISTRIBUTION_START", source=source_group_name, dry_run=dry_run)
    
    # 1. Coleta os operadores que estão no grupo atualmente
    operators_to_move = []
    try:
        for op in client.iter_group_operators(source_id):
            operators_to_move.append(op)
    except ArgusError as exc:
        log.critical(f"Falha ao listar operadores do grupo '{source_group_name}': {exc}")
        return False
        
    audit(log, "OPERATORS_COLLECTED", source=source_group_name, count=len(operators_to_move))

    if not operators_to_move:
        log.info("Nenhum operador encontrado no grupo. Nada a fazer.")
        return True

    # Preparar cache de destinos para evitar bater no `get_group_id` repetidamente 
    # e acabar travando nas threads.
    dest_group_cache = {}
    
    success_count = 0
    skipped_count = 0
    failed_count = 0

    # 2. Execução Concorrente da Movimentação
    def _worker(op):
        nonlocal success_count, skipped_count, failed_count
        op_login = op.get("login")
        op_id = op.get("id")
        
        # Valida se o operador existe no mapeamento
        target_group_name = mapping.get(op_login)
        if not target_group_name:
            # Não quebra, apenas registra e pula
            audit(log, "OPERATOR_NOT_MAPPED", logging.WARNING, operator=op_login, operator_id=op_id)
            skipped_count += 1
            return
            
        try:
            # Resolve o ID do grupo destino (usa cache para ser instantâneo)
            if target_group_name not in dest_group_cache:
                dest_group_cache[target_group_name] = client.get_group_id(target_group_name)
            target_id = dest_group_cache[target_group_name]
            
            if dry_run:
                audit(log, "OPERATOR_WOULD_MOVE", operator=op_login, target=target_group_name)
                success_count += 1
                return
                
            # Dispara a movimentação
            client.move_operator(op_id, source_id, target_id)
            audit(log, "OPERATOR_MOVED", operator=op_login, target=target_group_name)
            success_count += 1
            
        except GroupNotFoundError as exc:
            audit(log, "TARGET_GROUP_NOT_FOUND", logging.ERROR, operator=op_login, target=target_group_name, error=str(exc))
            failed_count += 1
        except ArgusAuthError:
            raise
        except Exception as exc:
            audit(log, "OPERATOR_MOVE_FAILED", logging.ERROR, operator=op_login, error=str(exc))
            failed_count += 1

    # Despacha os requests simultaneamente
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            futures = [executor.submit(_worker, op) for op in operators_to_move]
            concurrent.futures.wait(futures)
        except ArgusAuthError:
            log.critical("Processamento interrompido: Erro de Autenticação (Token Inválido).")
            return False

    log.info(f"RESUMO DA REDISTRIBUIÇÃO | Movidos: {success_count} | Pulados (sem mapa): {skipped_count} | Falhas: {failed_count}")
    return failed_count == 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Redistribuição de Operadores")
    ap.add_argument("--map", required=True, help="Arquivo JSON com o de-para de login -> grupo destino")
    ap.add_argument("--source", default="receptivo - ura", help="Grupo genérico onde os operadores caem")
    ap.add_argument("--config", default="config.json", help="Arquivo de configuração da API")
    ap.add_argument("--dry-run", action="store_true", help="Apenas simula a movimentação")
    args = ap.parse_args()

    log = setup_logger("operator_redistribution")

    try:
        cfg = load_config(args.config)
        # O ArgusClient já implementa o requests.Session() com o Pool de conexões otimizado (HTTPAdapter)
        client = ArgusClient(cfg, log) 
    except RuntimeError as exc:
        log.critical("Configuração inválida: %s", exc)
        return 2
        
    mapping = load_mapping(args.map, log)
    
    success = process_redistribution(client, args.source, mapping, log, args.dry_run)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
