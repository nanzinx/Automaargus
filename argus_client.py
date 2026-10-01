"""
argus_client.py
===============
Cliente HTTP genérico para a API do ARGUS.

Responsabilidades:
  * autenticação e sessão HTTP;
  * retry com backoff exponencial SOMENTE para falhas transitórias (timeout, 429, 5xx);
  * erros 4xx não sofrem retry (são erro de dado/permissão, repetir não resolve);
  * normalização das respostas (nomes de campo vêm do config.py);
  * métodos de alto nível usados pelas automações.
"""
import logging
import time
from typing import Iterator

import requests

from config import ArgusConfig


# ---------------------------------------------------------------------------
# Exceções
# ---------------------------------------------------------------------------
class ArgusError(Exception):
    """Erro base."""


class ArgusAuthError(ArgusError):
    """401/403 - token inválido ou sem permissão."""


class ArgusAPIError(ArgusError):
    """Resposta 4xx/5xx da API."""

    def __init__(self, status_code: int, body: str, url: str = ""):
        super().__init__(f"HTTP {status_code} em {url}: {body[:300]}")
        self.status_code = status_code
        self.body = body


class ArgusUnavailableError(ArgusError):
    """API indisponível após todas as tentativas."""


class GroupNotFoundError(ArgusError):
    """Grupo (origem ou destino) não encontrado na discadora."""


# ---------------------------------------------------------------------------
# Cliente
# ---------------------------------------------------------------------------
class ArgusClient:
    RETRY_STATUS = {429, 500, 502, 503, 504}

    def __init__(self, cfg: ArgusConfig, logger: logging.Logger):
        self.cfg = cfg
        self.log = logger
        self.session = requests.Session()
        token = f"{cfg.auth_scheme} {cfg.token}".strip()
        self.session.headers.update({cfg.auth_header: token, "Content-Type": "application/json"})
        self._groups_cache: dict[str, str] | None = None

    # ------------------------- infraestrutura -------------------------
    def _request(self, key: str, path: dict | None = None, params: dict | None = None, body: dict | None = None):
        if key not in self.cfg.endpoints:
            raise ArgusError(f"Endpoint '{key}' não configurado.")
        method, template = self.cfg.endpoints[key]
        url = self.cfg.base_url.rstrip("/") + template.format(**(path or {}))

        last_error: Exception | None = None
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                resp = self.session.request(
                    method, url, params=params, json=body,
                    timeout=self.cfg.timeout, verify=self.cfg.verify_ssl,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                self.log.warning("Falha de rede (%s) tentativa %d/%d: %s", key, attempt, self.cfg.max_retries, exc)
                self._sleep(attempt)
                continue

            if resp.status_code in (401, 403):
                raise ArgusAuthError(f"HTTP {resp.status_code}: verifique ARGUS_TOKEN/permissões.")
            if resp.status_code in self.RETRY_STATUS:
                last_error = ArgusAPIError(resp.status_code, resp.text, url)
                self.log.warning("HTTP %s (%s) tentativa %d/%d", resp.status_code, key, attempt, self.cfg.max_retries)
                self._sleep(attempt)
                continue
            if resp.status_code >= 400:
                raise ArgusAPIError(resp.status_code, resp.text, url)

            return resp.json() if resp.content else {}

        raise ArgusUnavailableError(f"API indisponível após {self.cfg.max_retries} tentativas ({key}): {last_error}")

    def _sleep(self, attempt: int) -> None:
        time.sleep(self.cfg.backoff ** attempt)

    @staticmethod
    def _as_list(payload) -> list:
        """Aceita lista pura ou envelopes comuns: {"data": [...]}, {"items": [...]} etc."""
        if isinstance(payload, list):
            return payload
        for k in ("data", "items", "results", "contacts", "robots", "groups", "calls"):
            if isinstance(payload, dict) and isinstance(payload.get(k), list):
                return payload[k]
        return []

    # ------------------------- GRUPOS / CONTATOS -------------------------
    def get_group_id(self, group_name: str) -> str:
        """Resolve nome -> ID. Lança GroupNotFoundError se não existir."""
        if self._groups_cache is None:
            f = self.cfg.fields
            groups = self._as_list(self._request("groups_list"))
            self._groups_cache = {str(g[f["group_name"]]).strip().lower(): str(g[f["group_id"]]) for g in groups}
        gid = self._groups_cache.get(group_name.strip().lower())
        if gid is None:
            raise GroupNotFoundError(f"Grupo '{group_name}' não encontrado na discadora.")
        return gid

    def iter_group_contacts(self, group_id: str, tabulacao: str | None = None) -> Iterator[dict]:
        """Itera (paginado) pelos contatos de um grupo, opcionalmente filtrando por tabulação."""
        f, q = self.cfg.fields, self.cfg.query_params
        page = 1
        while True:
            params = {q["page"]: page, q["page_size"]: self.cfg.page_size}
            if tabulacao:
                params[q["tabulacao_filter"]] = tabulacao
            items = self._as_list(self._request("group_contacts", {"group_id": group_id}, params=params))
            if not items:
                return
            for item in items:
                yield {
                    "id": str(item[f["contact_id"]]),
                    "tabulacao": item.get(f["contact_tabulacao"]),
                    "raw": item,
                }
            if len(items) < self.cfg.page_size:
                return
            page += 1

    def get_contact_group(self, contact_id: str) -> str | None:
        """Retorna o ID do grupo atual do contato (usado para verificar a movimentação)."""
        data = self._request("contact_get", {"contact_id": contact_id})
        value = data.get(self.cfg.fields["contact_group"])
        return str(value) if value is not None else None

    def get_group_operators_status(self, group_id: str) -> dict:
        """
        Consulta a disponibilidade dos operadores do grupo.
        Retorna dicionário com contagens, ex: {"available": 0, "busy": 5, "total": 5}
        """
        # Exemplo de payload/estrutura esperada da API:
        # {
        #    "available_count": 0,
        #    "busy_count": 5,
        #    "logged_in": 5
        # }
        payload = self._request("group_operators", {"group_id": group_id})
        
        # Estrutura de retorno normalizada
        return {
            "available": payload.get("available_count", 0),
            "busy": payload.get("busy_count", 0),
            "total": payload.get("logged_in", 0)
        }

    def move_contact(self, contact_id: str, source_group_id: str, target_group_id: str) -> None:
        """
        Move o contato de grupo preservando histórico (o contato mantém o mesmo ID;
        só o vínculo com o grupo muda).

        Estratégias:
          native       -> 1 chamada de "mover" (preferível: atômica).
          copy_remove  -> adiciona ao destino e SÓ ENTÃO remove da origem
                          (se a adição falhar, nada é removido => sem perda de dados).
        """
        if self.cfg.move_strategy == "native":
            self._request("contact_move", {"contact_id": contact_id},
                          body={"source_group_id": source_group_id, "target_group_id": target_group_id})
        elif self.cfg.move_strategy == "copy_remove":
            self._request("group_contact_add", {"group_id": target_group_id}, body={"contact_id": contact_id})
            self._request("group_contact_del", {"group_id": source_group_id, "contact_id": contact_id})
        else:
            raise ArgusError(f"move_strategy inválida: {self.cfg.move_strategy}")

    # ------------------------- ROBÔS / CAMPANHAS -------------------------
    def list_robots(self) -> list[dict]:
        f = self.cfg.fields
        return [
            {
                "id": str(r[f["robot_id"]]),
                "name": r.get(f["robot_name"], ""),
                "status": str(r.get(f["robot_status"], "")).upper(),
            }
            for r in self._as_list(self._request("robots_list"))
        ]

    def get_robot(self, robot_id: str) -> dict:
        f = self.cfg.fields
        r = self._request("robot_get", {"robot_id": robot_id})
        return {"id": robot_id, "name": r.get(f["robot_name"], ""), "status": str(r.get(f["robot_status"], "")).upper()}

    def pause_robot(self, robot_id: str) -> None:
        self._request("robot_pause", {"robot_id": robot_id})

    def resume_robot(self, robot_id: str) -> None:
        self._request("robot_resume", {"robot_id": robot_id})

    def count_active_calls(self) -> int | None:
        """Nº de ligações em andamento. Retorna None se a API não oferecer esse endpoint."""
        if "active_calls" not in self.cfg.endpoints:
            return None
        payload = self._request("active_calls")
        if isinstance(payload, dict) and isinstance(payload.get("total"), int):
            return payload["total"]
        return len(self._as_list(payload))
