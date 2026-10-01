"""
pre_callback_shutdown.py  -  AUTOMAÇÃO 2: Desligamento preventivo dos robôs (pré-callback)
=========================================================================================
Pausa todos os robôs ativos ANTES da geração do callback / consolidação de dados e só
libera o callback quando houver confirmação de que nada mais está discando.

Modos de gatilho:
  --now                      executa imediatamente (ideal para cron/Agendador de Tarefas)
  --at 18:00 --lead-min 5    espera até 17:55 (callback às 18:00) e executa

Sequência de segurança:
  1. trava de execução (evita duas instâncias simultâneas);
  2. snapshot dos robôs ativos (para poder religar depois com --resume);
  3. PAUSA todos (para de originar novas chamadas);
  4. DRENAGEM: aguarda as ligações em andamento terminarem (se a API informar);
  5. CONFIRMAÇÃO por polling: relê o status de cada robô até todos estarem pausados,
     reenviando a pausa nos que resistirem e capturando robôs ligados por terceiros;
  6. só então executa o callback (--callback-cmd) ou retorna 0 para o pipeline seguir;
  7. se NÃO confirmou: NÃO libera o callback, loga CRITICAL e sai com código 2.

Exemplos:
  python pre_callback_shutdown.py --at 18:00 --lead-min 5 --callback-cmd "python gerar_callback.py" --resume
  python pre_callback_shutdown.py --now          # (cron: 55 17 * * 1-5)

Códigos de saída: 0 ok | 2 robôs não confirmados (callback bloqueado) | 3 callback falhou | 4 erro fatal
"""
import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from argus_client import ArgusAuthError, ArgusClient, ArgusError
from audit_log import audit, setup_logger
from config import load_config

TZ = ZoneInfo("America/Sao_Paulo")
LOCK_PATH = "pre_callback.lock"
STATE_PATH = "robots_snapshot.json"


# ---------------------------------------------------------------------------
# Trava de execução
# ---------------------------------------------------------------------------
class RunLock:
    """Lock por arquivo (O_EXCL). Lock mais antigo que `stale_seconds` é considerado órfão."""

    def __init__(self, path: str = LOCK_PATH, stale_seconds: int = 3600):
        self.path, self.stale = path, stale_seconds

    def __enter__(self):
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return self
            except FileExistsError:
                if time.time() - os.path.getmtime(self.path) > self.stale:
                    os.remove(self.path)          # lock órfão de execução anterior
                    continue
                raise RuntimeError(f"Já existe uma execução em andamento ({self.path}).")
        raise RuntimeError("Não foi possível obter o lock.")

    def __exit__(self, *exc):
        if os.path.exists(self.path):
            os.remove(self.path)


# ---------------------------------------------------------------------------
# Gatilho de tempo
# ---------------------------------------------------------------------------
def wait_until_trigger(callback_time: str, lead_min: int, log: logging.Logger) -> None:
    """Dorme até (horário do callback - antecedência). Se já passou da janela, executa na hora."""
    hh, mm = map(int, callback_time.split(":"))
    now = datetime.now(TZ)
    callback_dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    trigger_dt = callback_dt - timedelta(minutes=lead_min)

    if now >= callback_dt:
        raise RuntimeError(f"O horário do callback ({callback_time}) de hoje já passou.")
    if now >= trigger_dt:
        log.warning("Já dentro da janela de antecedência; executando imediatamente.")
        return

    audit(log, "WAITING_FOR_TRIGGER", trigger=trigger_dt.isoformat(), callback=callback_dt.isoformat())
    while (remaining := (trigger_dt - datetime.now(TZ)).total_seconds()) > 0:
        time.sleep(min(30, remaining))


# ---------------------------------------------------------------------------
# Etapas do desligamento
# ---------------------------------------------------------------------------
def pause_robots(client: ArgusClient, robots: list[dict], log: logging.Logger) -> list[str]:
    """Envia pausa para cada robô. Retorna IDs que falharam no envio."""
    failed = []
    for r in robots:
        try:
            client.pause_robot(r["id"])
            audit(log, "ROBOT_PAUSE_SENT", robot_id=r["id"], name=r["name"])
        except ArgusAuthError:
            raise
        except ArgusError as exc:
            failed.append(r["id"])
            audit(log, "ROBOT_PAUSE_FAILED", logging.ERROR, robot_id=r["id"], name=r["name"], error=str(exc))
    return failed


def drain_calls(client: ArgusClient, timeout: int, poll: int, log: logging.Logger) -> bool:
    """Espera as ligações em andamento terminarem. True = zero ligações (ou API sem esse dado)."""
    deadline = time.monotonic() + timeout
    while True:
        active = client.count_active_calls()
        if active is None:
            log.warning("API não informa ligações ativas; etapa de drenagem ignorada.")
            return True
        audit(log, "DRAIN_POLL", active_calls=active)
        if active == 0:
            return True
        if time.monotonic() > deadline:
            audit(log, "DRAIN_TIMEOUT", logging.ERROR, active_calls=active)
            return False
        time.sleep(poll)


def confirm_all_paused(client: ArgusClient, timeout: int, poll: int, max_repause: int,
                       log: logging.Logger) -> list[dict]:
    """
    Polling até nenhum robô estar em estado "rodando".
    Relê a lista COMPLETA a cada ciclo (pega robôs ligados por outra pessoa no meio do processo).
    Retorna a lista de robôs ainda ativos ([] = confirmado).
    """
    running = {s.upper() for s in client.cfg.running_states}
    deadline = time.monotonic() + timeout
    repauses = 0
    while True:
        pending = [r for r in client.list_robots() if r["status"] in running]
        audit(log, "CONFIRM_POLL", still_running=[r["id"] for r in pending])
        if not pending:
            return []
        if time.monotonic() > deadline:
            return pending
        if repauses < max_repause:
            repauses += 1
            audit(log, "REPAUSE_ATTEMPT", attempt=repauses, robots=[r["id"] for r in pending])
            pause_robots(client, pending, log)
        time.sleep(poll)


def resume_from_snapshot(client: ArgusClient, log: logging.Logger) -> None:
    if not os.path.exists(STATE_PATH):
        log.warning("Sem snapshot para religar robôs.")
        return
    with open(STATE_PATH, encoding="utf-8") as fh:
        snapshot = json.load(fh)
    for r in snapshot:
        try:
            client.resume_robot(r["id"])
            audit(log, "ROBOT_RESUMED", robot_id=r["id"], name=r["name"])
        except ArgusError as exc:
            audit(log, "ROBOT_RESUME_FAILED", logging.ERROR, robot_id=r["id"], error=str(exc))


def notify_failure(message: str, log: logging.Logger) -> None:
    """GANCHO de alerta: plugue aqui Slack/Teams/e-mail/WhatsApp. Por ora só loga."""
    log.critical("ALERTA: %s", message)


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="ARGUS - desligamento preventivo de robôs pré-callback")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--now", action="store_true", help="Executa imediatamente")
    g.add_argument("--at", metavar="HH:MM", help="Horário do callback (America/Sao_Paulo)")
    ap.add_argument("--lead-min", type=int, default=5, help="Antecedência em minutos (com --at)")
    ap.add_argument("--drain-timeout", type=int, default=180, help="Máx. s esperando ligações encerrarem")
    ap.add_argument("--confirm-timeout", type=int, default=120, help="Máx. s esperando confirmação de pausa")
    ap.add_argument("--poll", type=int, default=5, help="Intervalo de polling (s)")
    ap.add_argument("--max-repause", type=int, default=3, help="Reenvios de pausa para robôs resistentes")
    ap.add_argument("--callback-cmd", help="Comando do callback, executado só após confirmação")
    ap.add_argument("--resume", action="store_true", help="Religa os robôs ao fim do callback")
    ap.add_argument("--dry-run", action="store_true", help="Só lista o que seria pausado")
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()

    log = setup_logger("pre_callback")
    try:
        cfg = load_config(args.config)
    except RuntimeError as exc:
        log.critical("Configuração inválida: %s", exc)
        return 4
    client = ArgusClient(cfg, log)

    try:
        with RunLock():
            if args.at:
                wait_until_trigger(args.at, args.lead_min, log)

            # 2) Snapshot dos robôs ativos
            running = {s.upper() for s in cfg.running_states}
            active = [r for r in client.list_robots() if r["status"] in running]
            with open(STATE_PATH, "w", encoding="utf-8") as fh:
                json.dump(active, fh, ensure_ascii=False, indent=2)
            audit(log, "SHUTDOWN_START", robots=[r["id"] for r in active], dry_run=args.dry_run)

            if args.dry_run:
                log.info("DRY-RUN: pausaria %d robô(s): %s", len(active), [r["name"] for r in active])
                return 0

            # 3) Pausa (para de originar novas ligações)
            pause_robots(client, active, log)

            # 4) Drenagem das ligações em andamento
            drained = drain_calls(client, args.drain_timeout, args.poll, log)

            # 5) Confirmação por polling
            still_running = confirm_all_paused(client, args.confirm_timeout, args.poll, args.max_repause, log)

            if still_running or not drained:
                msg = (f"Callback BLOQUEADO. Robôs ainda ativos: {[r['id'] for r in still_running]} | "
                       f"ligações drenadas: {drained}")
                audit(log, "SHUTDOWN_NOT_CONFIRMED", logging.CRITICAL, still_running=still_running, drained=drained)
                notify_failure(msg, log)
                return 2

            audit(log, "SHUTDOWN_CONFIRMED", robots_paused=len(active))

            # 6) Callback liberado
            exit_code = 0
            if args.callback_cmd:
                audit(log, "CALLBACK_START", cmd=args.callback_cmd)
                proc = subprocess.run(args.callback_cmd, shell=True)
                audit(log, "CALLBACK_END", returncode=proc.returncode)
                if proc.returncode != 0:
                    notify_failure(f"Callback terminou com código {proc.returncode}", log)
                    exit_code = 3

            if args.resume:
                resume_from_snapshot(client, log)
            return exit_code

    except ArgusAuthError as exc:
        log.critical("Erro de autenticação: %s", exc)
        return 4
    except (ArgusError, RuntimeError) as exc:
        log.critical("Erro fatal: %s", exc)
        notify_failure(str(exc), log)
        return 4


if __name__ == "__main__":
    sys.exit(main())
