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
- `POST /admin/config/reload` — recarrega a config ativa (demonstra "nova regra sem redeploy"). Aceita opcionalmente um corpo JSON `{"rules": [...], "profile": {...}}` (`profile` é opcional, cai no perfil ativo se omitido) para aplicar novas regras direto via curl; sem corpo, recarrega o que já estiver staged (ex.: pelo `demo`), ou não faz nada se nada estiver staged.
  ```bash
  curl -s localhost:8080/admin/config/reload -d '{"rules": [...regras completas, incl. as já ativas...]}' | jq
  ```

Coleções Postman e Insomnia prontas (todos os endpoints acima, com exemplos e
descrições) em [`docs/api-clients/`](docs/api-clients/README.md).

## O que cada pacote faz

| Pacote | Responsabilidade |
|---|---|
| `internal/domain` | Entidades e value objects, sem dependências externas |
| `internal/ports` | Interfaces do lado do consumidor: stores, config, sink |
| `internal/config` | Parsing/validação dos perfis de config (JSON embutido via `go:embed`) |
| `internal/engine` | Avaliação de regras (`expr`) e agregação de severidade/score |
| `internal/pipeline` | `Processor`: idempotência → janela/risco → regras → decisão → emissão |
| `internal/pipeline/dispatch` | `dispatch.Pool`: ordena por `hash(customer_id) % N` |
| `internal/adapters/memory` | Implementações em memória de todas as portas, com toggles de falha |
| `internal/adapters/httpapi` | Adaptador HTTP fino: sem lógica de detecção |
| `internal/loadgen` | Gerador de massa sintética + cliente de carga interno |
| `cmd/motor` | Wiring e subcomandos `serve`/`demo`/`loadtest` |

## Como funciona a avaliação

Cada transação passa pelo `pipeline.Processor`, que resolve idempotência,
janela e risco do cliente, e então delega a decisão ao `engine.Evaluator`
(`internal/engine/evaluator.go`):

1. **Monta o ambiente da regra.** Para cada regra habilitada do perfil de
   config ativo, o `Evaluator` monta um `map[string]any` com os dados que a
   regra pode referenciar: sempre os campos da transação (`amount`,
   `currency`, `channel`, `device_id`, `customer_id`); `risk.limite_valor` e
   `risk.nivel` se a regra declarar `requires: [customer_risk]`; e
   `window.count_channel_*` (contagem por canal na janela `span_seconds`) se
   a regra declarar `requires: [window]`.
2. **Compila e roda a condição.** A `condition` da regra é uma expressão
   [`expr`](https://github.com/expr-lang/expr) (ex.:
   `amount > risk.limite_valor && channel == "pix"`), compilada uma vez por
   `rule_id@version` e cacheada em memória (`programs map[string]*vm.Program`,
   protegido por `sync.RWMutex`) — reload de config sem redeploy não recompila
   regras que não mudaram. `expr.Run` executa o programa contra o ambiente e
   espera um `bool` de volta.
3. **Agrega severidade e score.** Cada regra disparada contribui com uma
   `Severity` (`baixa|media|alta|critica`) e uma `category`. A severidade
   final do veredito é o máximo entre as regras disparadas; o score numérico
   é `SeverityWeight(severidade_max)*0.7 + média(SeverityWeight das regras
   disparadas)*0.3` (`internal/engine/score.go`), com pesos
   `baixa=0.25, media=0.5, alta=0.75, critica=1.0`. Um alerta é emitido
   quando o score cruza `score_minimo_alerta` do perfil ativo (ver
   "Score e threshold de alerta" abaixo).
4. **Degrada, nunca falha.** Se `windowAvailable` é `false` (checado antes)
   ou o `WindowStore` cai durante a leitura (checado de novo, defensivamente,
   dentro do `Evaluator`), regras que dependem de `window` são puladas e o
   `Result.WindowDegraded` sobe para o `Processor`, que marca o veredito como
   `evaluation: "partial"` — regras que só dependem de `event` continuam
   avaliando normalmente. `customer_risk` nunca degrada: cai para o
   `default_customer_risk` global se o cliente não tiver perfil.

### Frameworks e bibliotecas usadas, e por quê

O projeto é deliberadamente magro em dependências — só duas diretas
(`go.mod`), ambas escolhidas para um problema específico:

| Biblioteca | Papel | Por quê |
|---|---|---|
| [`github.com/expr-lang/expr`](https://github.com/expr-lang/expr) | Motor de expressões que compila e roda as `condition` das regras de detecção | É o único jeito prático de atender ao requisito "novas regras sem redeploy": regras vêm como strings de config (JSON, via `go:embed`, recarregáveis em runtime por `POST /admin/config/reload`) em vez de `if`s compilados no binário. `expr` compila cada condição para um `*vm.Program` bytecode-like, então avaliar é rápido mesmo cacheado por `rule_id@version` — sem custo de reparsear a cada transação. Alternativa descartada: interpretar regras "na mão" (parser próprio) — reinventar a roda para o mesmo resultado, com mais superfície de bugs. |
| [`github.com/stretchr/testify`](https://github.com/stretchr/testify) | Asserções (`assert`/`require`) nos testes | Só em `_test.go`, nunca em código de produção. Reduz ruído de `if err != nil { t.Fatalf(...) }` repetido nos ~10 pacotes com teste, sem trazer nenhum comportamento em runtime — é dependência só de build de teste, não do binário final. |

Nenhum framework web, ORM ou driver de banco é usado: `internal/adapters/httpapi`
é construído só com `net/http` da stdlib (a API é fina o bastante para não
justificar um router/framework), e todo o resto de I/O externo (janela,
risco, config, sink de alertas) é mockado em memória em
`internal/adapters/memory` — não há banco de dados nem fila reais no
projeto, então nenhuma lib de acesso a dados foi necessária.

## Decisões de design

- **Ports & adapters**: cada dependência externa (janela, risco, config,
  sink de alertas) é uma interface definida em `internal/ports` (lado do
  consumidor), com uma única implementação em memória plugável em
  `internal/adapters/memory`.
- **Lógica única no `Processor`**: a API HTTP alimenta o `dispatch.Pool`,
  que por sua vez chama sempre o mesmo `pipeline.Processor.Process`.
  Nenhuma regra de negócio existe na camada HTTP. (Uma versão anterior
  incluía uma segunda porta de entrada mockando um consumidor Kafka
  particionado por `customer_id`; foi removida para manter o escopo focado
  em API + testes de carga — o `dispatch.Pool` continua desenhado para
  múltiplas portas de entrada convergirem nele, então reintroduzir uma
  fonte de eventos no futuro não exigiria mudar o `Processor`.)
- **Ordem por `customer_id`**: o `dispatch.Pool` roteia cada transação para
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
  demo, sem qualquer dependência de infraestrutura real. Indisponibilidade é
  tratada tanto no momento em que o `Processor` verifica a janela quanto,
  defensivamente, durante a leitura de cada regra (`internal/engine`) — o
  `WindowStore` pode cair *entre* essas duas checagens sob tráfego real (é
  exatamente o que `POST /admin/window/down` faz ao vivo), e nos dois casos
  o resultado é sempre degradação graciosa (`evaluation: "partial"`), nunca
  um erro fatal.
- **Score e threshold de alerta**: `score = peso(severidade_max) * 0.7 +
  média(pesos das regras disparadas) * 0.3`, com pesos
  `baixa=0.25, media=0.5, alta=0.75, critica=1.0`. `score_minimo_alerta` no
  perfil padrão é **0.5** (não 0.6): como `valor-atipico` emite severidade
  `media` (peso 0.5), um único disparo dela soma exatamente `0.5*0.7 +
  0.5*0.3 = 0.5` — em 0.6 essa regra jamais alertaria sozinha, o que
  contradiz o próprio roteiro de demo do enunciado ("uma transação de valor
  alto que dispara valor-atipico"). Corrigido para 0.5 para que o cenário
  primário do enunciado funcione como descrito.
- **Resiliência do pool**: `dispatch.Pool`'s workers recuperam de panics em
  `job.Run` (uma regra malformada ou um bug não derruba o processo inteiro,
  nem trava a fila daquele worker).
- **Logging estruturado**: `pipeline.Processor` recebe um `*slog.Logger` e
  gera um `trace_id` por transação, correlacionando as linhas de log de
  cada requisição (`customer_id`, `transaction_id`, resultado).

## Sobre os números de desempenho

Os números de `loadtest` (TPS, p50/p95/p99) refletem **este processo local
rodando contra mocks em memória** — não a infraestrutura real (Kafka, CC,
EK7, rede). Servem para demonstrar que o paralelismo por `customer_id`
sustenta carga, não como benchmark de produção.
