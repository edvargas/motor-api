# Motor de Detecção de Transações Suspeitas

Exercício de arquitetura (entrevista de engenharia sênior): motor de detecção
de transações suspeitas em Go, com **toda entrada e toda consulta externa
mockadas em memória**. Roda localmente com `go run`, sem Kafka, sem AWS, sem
banco de dados.

## Como rodar

```bash
go build ./...          # compila tudo
make test                # go test -race ./... (ou: go test -race ./...)

make run                 # sobe a API HTTP em :8080
make demo                # roteiro scriptado: risco por cliente, janela,
                          # idempotência, modo degradado, hot reload
make loadtest             # gera 100k transações sintéticas e mede TPS/p99
```

`loadtest` aceita flags, ex.: `go run ./cmd/motor loadtest -n 50000 -customers 2000 -concurrency 128 -seed 7 -dump out.jsonl`.
`out.jsonl` (se usado) pode ser reproduzido depois com `curl`/`vegeta`/`k6`
contra `POST /transactions`.

## Endpoints

- `POST /transactions` — uma transação, veredito síncrono.
  ```bash
  curl -s localhost:8080/transactions -d '{
    "customer_id":"c1","transaction_id":"t1","amount":6000,
    "currency":"BRL","channel":"card","device_id":"d1",
    "captured_at":"2026-08-24T14:03:11Z"
  }' | jq
  ```
- `POST /transactions/batch` — array de transações, resumo agregado (total, alertas, duplicatas, parciais, p50/p95/p99, TPS).
- `GET /healthz` — liveness.
- `GET /metrics` — snapshot JSON dos contadores.
- `POST /admin/window/down` / `/admin/window/up` — alterna a indisponibilidade simulada do `WindowStore` (demonstra modo degradado).
- `POST /admin/config/reload` — recarrega a config ativa (demonstra "nova regra sem redeploy").

## O que cada pacote faz

| Pacote | Responsabilidade |
|---|---|
| `internal/domain` | Entidades e value objects, sem dependências externas |
| `internal/ports` | Interfaces do lado do consumidor: source, stores, config, sink |
| `internal/config` | Parsing/validação dos perfis de config (JSON embutido via `go:embed`) |
| `internal/engine` | Avaliação de regras (`expr`) e agregação de severidade/score |
| `internal/pipeline` | `Processor`: idempotência → janela/risco → regras → decisão → emissão |
| `internal/pipeline/dispatch` | `WorkerPool`: ordena por `hash(customer_id) % N` |
| `internal/adapters/memory` | Implementações em memória de todas as portas, com toggles de falha |
| `internal/adapters/httpapi` | Adaptador HTTP fino: sem lógica de detecção |
| `internal/loadgen` | Gerador de massa sintética + cliente de carga interno |
| `cmd/motor` | Wiring e subcomandos `serve`/`demo`/`loadtest` |

## Decisões de design

- **Ports & adapters**: cada dependência externa (fonte de eventos, janela,
  risco, config, sink de alertas) é uma interface definida em `internal/ports`
  (lado do consumidor), com uma única implementação em memória plugável em
  `internal/adapters/memory`.
- **Lógica única no `Processor`**: tanto a API HTTP quanto a fonte mockada
  (que simula um consumidor Kafka particionado por `customer_id`) alimentam
  o mesmo `dispatch.Pool`, que por sua vez chama sempre o mesmo
  `pipeline.Processor.Process`. Nenhuma regra de negócio existe na camada
  HTTP.
- **Ordem por `customer_id`**: o `WorkerPool` roteia cada transação para
  `hash(customer_id) % N`, garantindo que transações do mesmo cliente nunca
  sejam reordenadas, enquanto clientes diferentes processam em paralelo —
  tanto na API síncrona quanto no endpoint de lote (que reparte por
  `customer_id`, não por índice do array).
- **Modo degradado via `requires`**: cada regra declara de quais camadas
  depende (`event`, `window`, `customer_risk`). Se o `WindowStore` estiver
  indisponível, regras que dependem de `window` são puladas e o alerta
  resultante (se houver) sai com `evaluation: "partial"` e
  `degraded: ["window"]`; regras que só dependem de `event` continuam
  funcionando normalmente. `customer_risk` nunca aparece em `degraded`
  porque sempre cai para o `default_customer_risk` global.
- **Regras por config, sem redeploy**: condições são expressões `expr`
  compiladas e cacheadas por `rule_id@version`; `ConfigProvider.Reload`
  revalida a config antes de trocar a versão ativa, mantendo a última
  válida em caso de erro.
- **Mocks plugáveis com toggles de falha**: `WindowStore.SetDown(bool)` e
  `RiskStore.SetDown(bool)` simulam indisponibilidade para testes e para a
  demo, sem qualquer dependência de infraestrutura real.

## Sobre os números de desempenho

Os números de `loadtest` (TPS, p50/p95/p99) refletem **este processo local
rodando contra mocks em memória** — não a infraestrutura real (Kafka, CC,
EK7, rede). Servem para demonstrar que o paralelismo por `customer_id`
sustenta carga, não como benchmark de produção.
