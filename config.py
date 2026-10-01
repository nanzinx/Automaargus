"""
config.py
=========
Configuração central das automações ARGUS.

Tudo que depende da API real da discadora fica AQUI (endpoints e nomes de campos).
Quando você tiver a documentação do ARGUS, ajuste apenas este arquivo (ou o config.json)
e os scripts de automação continuam iguais.

Segredos (token) vêm SEMPRE de variável de ambiente, nunca do código:
    export ARGUS_BASE_URL="https://argus.suaempresa.com.br/api/v1"
    export ARGUS_TOKEN="seu_token_aqui"
"""
import json
import os
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# ENDPOINTS  -> {"chave_interna": ("METODO", "/caminho/{placeholders}")}
# ATENÇÃO: valores abaixo são HIPOTÉTICOS. Ajustar conforme a documentação do ARGUS.
# ---------------------------------------------------------------------------
DEFAULT_ENDPOINTS = {
    # --- Automação 1: grupos/contatos ---
    "groups_list":        ("GET",    "/groups"),
    "group_contacts":     ("GET",    "/groups/{group_id}/contacts"),
    "contact_get":        ("GET",    "/contacts/{contact_id}"),
    "contact_move":       ("POST",   "/contacts/{contact_id}/move"),            # estratégia "native"
    "group_contact_add":  ("POST",   "/groups/{group_id}/contacts"),            # estratégia "copy_remove"
    "group_contact_del":  ("DELETE", "/groups/{group_id}/contacts/{contact_id}"),  # estratégia "copy_remove"
    # --- Automação 2: robôs/campanhas ---
    "robots_list":        ("GET",    "/robots"),
    "robot_get":          ("GET",    "/robots/{robot_id}"),
    "robot_pause":        ("POST",   "/robots/{robot_id}/pause"),
    "robot_resume":       ("POST",   "/robots/{robot_id}/resume"),
    "active_calls":       ("GET",    "/calls/active"),   # opcional: se não existir, remova a chave
}

# Nomes dos campos no JSON de resposta da API  -> nome usado internamente
DEFAULT_FIELDS = {
    "group_id": "id",
    "group_name": "name",
    "contact_id": "id",
    "contact_tabulacao": "tabulacao",   # último status/tabulação do contato
    "contact_group": "group_id",
    "robot_id": "id",
    "robot_name": "name",
    "robot_status": "status",
}

# Nomes de parâmetros de query usados nas listagens
DEFAULT_QUERY_PARAMS = {
    "page": "page",
    "page_size": "page_size",
    "tabulacao_filter": "tabulacao",    # filtro server-side (se a API suportar)
}

# Estados dos robôs (sempre comparados em MAIÚSCULAS)
DEFAULT_RUNNING_STATES = ["ACTIVE", "RUNNING", "DIALING", "ATIVO", "EM_EXECUCAO"]
DEFAULT_PAUSED_STATES = ["PAUSED", "STOPPED", "INACTIVE", "PAUSADO", "PARADO", "INATIVO"]


@dataclass
class ArgusConfig:
    base_url: str = ""
    token: str = ""
    auth_header: str = "Authorization"
    auth_scheme: str = "Bearer"          # use "" se a API aceitar o token puro (ex.: X-API-Key)
    timeout: int = 20                    # segundos por requisição
    max_retries: int = 3                 # tentativas em falha transitória (timeout, 5xx, 429)
    backoff: float = 1.5                 # espera = backoff ** tentativa
    verify_ssl: bool = True
    page_size: int = 200
    move_strategy: str = "native"        # "native" | "copy_remove"
    endpoints: dict = field(default_factory=lambda: dict(DEFAULT_ENDPOINTS))
    fields: dict = field(default_factory=lambda: dict(DEFAULT_FIELDS))
    query_params: dict = field(default_factory=lambda: dict(DEFAULT_QUERY_PARAMS))
    running_states: list = field(default_factory=lambda: list(DEFAULT_RUNNING_STATES))
    paused_states: list = field(default_factory=lambda: list(DEFAULT_PAUSED_STATES))


def load_config(path: str | None = None) -> ArgusConfig:
    """Carrega config: defaults -> config.json (opcional) -> variáveis de ambiente."""
    cfg = ArgusConfig()

    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        for key, value in data.items():
            if key == "endpoints":
                # JSON não tem tupla: aceitar ["GET", "/path"]
                cfg.endpoints.update({k: tuple(v) for k, v in value.items()})
            elif key in ("fields", "query_params"):
                getattr(cfg, key).update(value)
            elif hasattr(cfg, key):
                setattr(cfg, key, value)

    cfg.base_url = os.getenv("ARGUS_BASE_URL", cfg.base_url)
    cfg.token = os.getenv("ARGUS_TOKEN", cfg.token)

    if not cfg.base_url or not cfg.token:
        raise RuntimeError("Defina ARGUS_BASE_URL e ARGUS_TOKEN (variáveis de ambiente).")
    return cfg
