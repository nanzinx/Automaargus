# Automações ARGUS

Duas automações em Python (3.10+) sobre uma camada de cliente configurável.

| Arquivo | Função |
|---|---|
| `config.py` | Endpoints, nomes de campos e estados dos robôs (**ajustar aqui** conforme a API real) |
| `argus_client.py` | Cliente HTTP: auth, retry/backoff, exceções, normalização |
| `audit_log.py` | Logs de auditoria (arquivo rotativo + console, linhas `AUDIT {json}`) |
| `group_transfer.py` | **Automação 1** – mover contatos entre grupos por tabulação |
| `pre_callback_shutdown.py` | **Automação 2** – pausar robôs e confirmar antes do callback |

## Setup
```bash
pip install -r requirements.txt
export ARGUS_BASE_URL="https://argus.suaempresa.com.br/api/v1"
export ARGUS_TOKEN="..."
cp config.example.json config.json      # ajuste endpoints/campos
```

## Automação 1
```bash
python group_transfer.py --rules rules.example.json --dry-run   # simula
python group_transfer.py --rules rules.example.json             # executa
```
Falhas individuais vão para `falhas_transferencia_AAAAMMDD_HHMMSS.csv` para reprocessamento.

## Automação 2
```bash
python pre_callback_shutdown.py --now --dry-run
python pre_callback_shutdown.py --at 18:00 --lead-min 5 --callback-cmd "python gerar_callback.py" --resume
# cron: 55 17 * * 1-5  cd /opt/argus && python pre_callback_shutdown.py --now --callback-cmd "..."
```
Se algum robô não confirmar a pausa, o callback **não** é executado (saída 2).

## O que precisa ser validado na documentação do ARGUS
1. URL base e esquema de autenticação (Bearer, API-Key, Basic?).
2. Existe endpoint nativo de "mover contato"? Se não, use `"move_strategy": "copy_remove"`.
3. Existe filtro por tabulação na listagem de contatos? (o código também filtra no cliente.)
4. Endpoints e status de pausa/retomada de robôs/campanhas, e se há endpoint de ligações ativas.
5. Se a API só oferece banco de dados/AMI/webhook em vez de REST, só `argus_client.py` precisa ser reescrito.

## Exemplos de payload (hipotéticos)
```http
POST /contacts/12345/move
{"source_group_id": "10", "target_group_id": "22"}

POST /robots/7/pause        (sem corpo)
```
