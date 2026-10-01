"""
audit_log.py
============
Logs para auditoria: console + arquivo rotativo.
Cada ação relevante gera uma linha "AUDIT {json}" fácil de filtrar com grep/jq.
"""
import json
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:                      # evita duplicar handlers
        return logger
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f"{name}.log"), maxBytes=5_000_000, backupCount=10, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    console = logging.StreamHandler()
    console.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console)
    return logger


def audit(logger: logging.Logger, action: str, level: int = logging.INFO, **fields) -> None:
    """Registra uma ação auditável. Ex.: audit(log, "CONTACT_MOVED", contact_id=1, to="B")."""
    logger.log(level, "AUDIT %s", json.dumps({"action": action, **fields}, ensure_ascii=False, default=str))
