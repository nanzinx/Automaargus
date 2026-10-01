"""
monitor_ura_receptivo.py
========================
Script preventivo e contínuo para monitoramento da fila "receptivo - ura".

Regra de negócio:
  Monitora a disponibilidade de operadores no grupo. Quando TODOS os operadores
  estiverem em atendimento (ou seja, operadores disponíveis == 0), este script
  acorda e imediatamente envia o comando de pausa (desligamento) para todos os robôs ativos.

  Essa ação ocorre de forma estritamente reativa, ignorando relógios ou agendamentos,
  e deve preceder a execução da rotina normal de pré-callback para evitar novas chamadas
  caindo na fila lotada.

Uso típico (antes do script principal de callback):
  python monitor_ura_receptivo.py --group "receptivo - ura" --poll 0.5
"""

import argparse
import concurrent.futures
import json
import logging
import os
import sys
import time
from datetime import datetime

from argus_client import ArgusAuthError, ArgusClient, ArgusError
from audit_log import audit, setup_logger
from config import load_config
from pre_callback_shutdown import RunLock, STATE_PATH


def fast_pause_robots(client: ArgusClient, robots: list[dict], log: logging.Logger) -> list[str]:
    """
    Envia o comando de pausa simultaneamente para todos os robôs (em threads paralelas).
    Isso reduz a latência para a escala de milissegundos.
    """
    failed = []
    
    def _pause(r):
        try:
            client.pause_robot(r["id"])
            audit(log, "ROBOT_PAUSE_SENT", robot_id=r["id"], name=r["name"])
            return None
        except Exception as exc:
            audit(log, "ROBOT_PAUSE_FAILED", logging.ERROR, robot_id=r["id"], name=r["name"], error=str(exc))
            return r["id"]

    # Usa até 20 threads (ou o número de robôs) para disparar as chamadas instantaneamente e em paralelo
    workers = max(1, min(20, len(robots)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = executor.map(_pause, robots)
        for res in results:
            if res:
                failed.append(res)
                
    return failed


def monitor_queue(client: ArgusClient, group_id: str, group_name: str, poll_interval: float, log: logging.Logger) -> list[dict]:
    """
    Faz o polling na fila e mantém a lista de robôs ativos pré-carregada para agir instantaneamente.
    Retorna a lista de robôs que devem ser pausados.
    """
    log.info(f"Iniciando monitoramento ultrarrápido da fila '{group_name}'. Intervalo: {poll_interval}s")
    
    running_states = {s.upper() for s in client.cfg.running_states}
    active_robots_cache = []
    last_robots_update = 0
    
    while True:
        now = time.time()
        # Pre-cache: Atualiza a lista de robôs a cada 15 segundos em background
        # Isso evita gastar milissegundos preciosos buscando a lista DEPOIS da fila lotar
        if now - last_robots_update > 15:
            try:
                active_robots_cache = [r for r in client.list_robots() if r["status"] in running_states]
                last_robots_update = now
            except ArgusError as exc:
                log.error(f"Falha ao atualizar cache de robôs ativos: {exc}")
        
        try:
            status = client.get_group_operators_status(group_id)
            available = status.get("available", 0)
            total = status.get("total", 0)
            
            # Condição crítica de negócio
            if available == 0 and total > 0:
                log.warning(f"Condição atingida na fila '{group_name}'! 0 operadores disponíveis de {total}.")
                return active_robots_cache
                
        except ArgusError as exc:
            # Em caso de erro transitório, o polling aguarda e tenta novamente sem travar
            pass
            
        time.sleep(poll_interval)


def shutdown_robots_preventively(client: ArgusClient, log: logging.Logger, dry_run: bool, active_robots: list[dict]):
    """
    Executa a pausa paralela imediatamente e SÓ DEPOIS grava o snapshot (para poupar milissegundos).
    """
    if not active_robots:
        log.info("Nenhum robô no cache estava ativo. Nada a pausar.")
        return True

    if dry_run:
        log.info("DRY-RUN: pausaria %d robô(s): %s", len(active_robots), [r["name"] for r in active_robots])
        return True

    # 1. PAUSA IMEDIATA EM PARALELO (antes de perder tempo com arquivos I/O)
    audit(log, "PREVENTIVE_SHUTDOWN_START", robots_to_pause=len(active_robots))
    failed = fast_pause_robots(client, active_robots, log)
    
    # 2. GRAVAÇÃO DO SNAPSHOT (Apenas depois de disparar o comando na API)
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as fh:
            json.dump(active_robots, fh, ensure_ascii=False, indent=2)
        audit(log, "PREVENTIVE_SHUTDOWN_SNAPSHOT", robots=[r["id"] for r in active_robots], file=STATE_PATH)
    except Exception as exc:
        log.warning(f"Não foi possível salvar o snapshot ({STATE_PATH}): {exc}")

    if failed:
        log.error(f"Falha ao pausar os robôs (IDs): {failed}")
        return False
        
    log.info("Todos os robôs ativos receberam o comando de pausa simultaneamente.")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="Monitoramento reativo da URA para desligamento da discadora")
    ap.add_argument("--group", default="receptivo - ura", help="Nome do grupo/fila a ser monitorado")
    # Intervalo de polling reduzido para meio segundo (para checar a fila constantemente)
    ap.add_argument("--poll", type=float, default=0.5, help="Intervalo de verificação da API (segundos)")
    ap.add_argument("--dry-run", action="store_true", help="Monitora, mas apenas avisa e não desliga os robôs")
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()

    log = setup_logger("monitor_ura")
    
    try:
        cfg = load_config(args.config)
        client = ArgusClient(cfg, log)
    except RuntimeError as exc:
        log.critical("Configuração inválida: %s", exc)
        return 4

    try:
        with RunLock("monitor_ura.lock"):
            log.info(f"Resolvendo ID para o grupo: {args.group}")
            group_id = client.get_group_id(args.group)
            
            # Trava no loop e volta instantaneamente a lista pré-carregada de robôs quando acionar
            active_robots = monitor_queue(client, group_id, args.group, args.poll, log)
            
            # Dispara simultaneamente os requests de parada
            success = shutdown_robots_preventively(client, log, args.dry_run, active_robots)
            return 0 if success else 2
                
    except ArgusAuthError as exc:
        log.critical("Erro de autenticação: %s", exc)
        return 4
    except Exception as exc:
        log.critical("Erro fatal na execução: %s", exc)
        return 4

    return 0


if __name__ == "__main__":
    sys.exit(main())
