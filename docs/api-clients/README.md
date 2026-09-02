# Clientes de API (Postman / Insomnia) — motor de detecção de transações suspeitas

Coleções prontas para a API HTTP exposta em `internal/adapters/httpapi` (rotas
definidas em `server.go`). Cobrem os 7 endpoints do serviço: submissão de
transações (unitária e em lote), observabilidade (`/healthz`, `/metrics`) e os
toggles administrativos (janela deslizante e reload de configuração). O
conteúdo e a organização (3 pastas: Transactions, Observability, Admin) são
os mesmos nas duas ferramentas.

## Arquivos

- `motor-deteccao.postman_collection.json` — coleção Postman.
- `motor-deteccao.postman_environment.json` — environment Postman `motor-deteccao local`, aponta para `http://localhost:8080`.
- `motor-deteccao.insomnia.json` — export Insomnia v4 (workspace + environment `local` + as mesmas 7 requisições).

## Como importar no Postman

1. Abra o Postman → **Import** → selecione os dois arquivos `motor-deteccao.postman_*.json` deste diretório.
2. Selecione o environment **motor-deteccao local** no seletor do canto superior direito.
3. Suba a API localmente: `make run` (sobe em `:8080`, conforme o `README.md` da raiz).
4. Rode as requisições da pasta **Transactions** primeiro para validar o fluxo básico.

## Como importar no Insomnia

1. Abra o Insomnia → **Application menu → Import/Export → Import Data → From File** → selecione `motor-deteccao.insomnia.json`.
2. Isso cria o workspace **Motor de Deteccao de Transacoes Suspeitas** com o environment **local** já configurado (`baseUrl=http://localhost:8080`).
3. Selecione o sub-environment **local** no seletor de environment do Insomnia.
4. Suba a API localmente: `make run` (sobe em `:8080`).
5. Rode as requisições da pasta **Transactions** primeiro para validar o fluxo básico.

Se você rodar a API em outra porta/host (por exemplo, o modo `loadtest`, que
sobe em `:18080` por padrão), edite a variável `baseUrl` no environment.

## Endpoints

### Transactions

| Request | Método | Path | Descrição |
|---|---|---|---|
| Submit transaction | `POST` | `/transactions` | Avalia uma transação síncrona pelo pool de dispatch + pipeline. |
| Submit transaction batch | `POST` | `/transactions/batch` | Avalia um array de transações em paralelo e retorna um resumo agregado. |

**Submit transaction** — corpo (`domain.Transaction`):

```json
{
  "customer_id": "cust-001",
  "transaction_id": "tx-001",
  "amount": 150.50,
  "currency": "BRL",
  "channel": "pix",
  "device_id": "device-abc",
  "geo": { "country": "BR", "lat": -23.5505, "lon": -46.6333 },
  "captured_at": "2026-09-01T12:00:00Z"
}
```

- `channel` aceita apenas `"pix"`, `"ted"` ou `"card"`.
- `customer_id`, `transaction_id`, `currency` e `captured_at` são obrigatórios; `amount` deve ser `> 0`. Qualquer violação retorna `400`.
- A chave de idempotência é `customer_id:transaction_id` — reenviar a mesma combinação retorna `{"status": "duplicate"}` em vez de reprocessar.

Respostas possíveis:

| Status | Corpo | Quando |
|---|---|---|
| 200 | `{"suspicious": false}` | Nenhuma regra disparou. |
| 200 | `{"suspicious": true, "alert": {...}}` | Ao menos uma regra disparou (`domain.Alert`, veja abaixo). |
| 200 | `{"status": "duplicate", "suspicious": false}` | `customer_id:transaction_id` já processado antes. |
| 400 | `{"error": "..."}` | Payload estruturalmente inválido. |
| 500 | `{"error": "internal error processing transaction"}` | Falha interna (store, publish, panic no worker). |

Formato de `alert` (`domain.Alert`):

```json
{
  "alert_id": "…",
  "customer_id": "cust-001",
  "transaction_id": "tx-001",
  "severity": "alta",
  "categories": ["velocidade"],
  "score": 0.82,
  "triggered_rules": [{ "rule_id": "velocidade-pix", "version": 4 }],
  "evaluation": "complete",
  "degraded": []
}
```

- `severity` é uma de `baixa`, `media`, `alta`, `critica`.
- `evaluation` é `complete` ou `partial` — `partial` indica que uma camada requerida (ex.: janela deslizante) estava indisponível no momento da avaliação.
- `degraded` lista as camadas que estavam indisponíveis (presente só quando `evaluation == "partial"`).

**Submit transaction batch** — corpo: um array JSON de `domain.Transaction` (mesmo formato acima). O particionamento interno é por `customer_id`: eventos do mesmo cliente são processados na ordem de submissão, entre clientes diferentes o processamento é paralelo.

Resposta 200 (`batchSummary`):

```json
{
  "total": 2,
  "alerts": 1,
  "duplicates": 0,
  "partials": 0,
  "latency_ms": { "p50": 1.2, "p95": 3.4, "p99": 4.0 },
  "tps": 950.5
}
```

### Observability

| Request | Método | Path | Descrição |
|---|---|---|---|
| Health check | `GET` | `/healthz` | Liveness simples: `200 {"status": "ok"}`. |
| Get metrics snapshot | `GET` | `/metrics` | Snapshot acumulado (contadores + percentis de latência) desde o boot do processo. |

`/metrics` agrega tanto tráfego de `/transactions` quanto de `/transactions/batch`:

```json
{
  "processed": 120,
  "duplicates": 2,
  "alerts": 15,
  "partials": 1,
  "by_category": { "velocidade": 10, "valor": 5 },
  "latency_ms": { "p50": 1.1, "p95": 3.0, "p99": 5.5 }
}
```

### Admin

| Request | Método | Path | Descrição |
|---|---|---|---|
| Force sliding window down | `POST` | `/admin/window/down` | Simula indisponibilidade da janela deslizante. |
| Restore sliding window | `POST` | `/admin/window/up` | Restaura a janela deslizante. |
| Reload config (rules + profile) | `POST` | `/admin/config/reload` | Substitui regras e/ou profile operacional em tempo real. |
| Reload staged config (no body) | `POST` | `/admin/config/reload` | Aplica um override já staged anteriormente (no-op se nada staged). |

Notas importantes:

- Os três endpoints de admin retornam **503** se o servidor não foi iniciado com `WindowStore`/`ConfigProvider` — isso é o caso do modo `loadtest` (`cmd/motor/loadtest.go`), que passa `nil` para ambos. O modo `serve` padrão (`make run`) tem os dois disponíveis.
- No reload de config, `rules` é o **conjunto completo** desejado, não um merge incremental — inclua também as regras que já estavam ativas e você quer manter.
- `profile` é opcional: se omitido, o profile atualmente ativo é mantido.
- O `version` retornado na resposta **não** é o `profile.version` do corpo enviado — é o contador interno de reloads do `ConfigProvider` (`internal/adapters/memory/config_provider.go`), que começa em `1` e incrementa a cada reload bem-sucedido, independentemente do conteúdo enviado.
- Formato de uma regra (`domain.RuleDef`), com exemplo real de `internal/config/default_rules.json`:

```json
{
  "rule_id": "velocidade-pix",
  "version": 4,
  "enabled": true,
  "requires": ["event", "window"],
  "window": { "span_seconds": 300, "type": "count" },
  "condition": "channel == \"pix\" && window.count_channel_pix > 8",
  "emits": { "severity": "alta", "category": "velocidade" }
}
```

- Formato do profile (`domain.OperationalProfile`), com exemplo real de `internal/config/default_profile.json`:

```json
{
  "version": 5,
  "default_customer_risk": { "limite_valor": 5000, "nivel": "padrao" },
  "thresholds": { "score_minimo_alerta": 0.5 }
}
```

## Referências

- Rotas: `internal/adapters/httpapi/server.go`
- Handlers e formatos de request/response: `internal/adapters/httpapi/handlers.go`
- Métricas: `internal/adapters/httpapi/metrics.go`
- Tipos de domínio: `internal/domain/transaction.go`, `internal/domain/alert.go`, `internal/domain/rule.go`, `internal/domain/severity.go`
- Regras/profile padrão: `internal/config/default_rules.json`, `internal/config/default_profile.json`
