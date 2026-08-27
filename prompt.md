# Prompt — App do motor de detecção (Go, tudo mockado)

> Cole o conteúdo abaixo (da linha `---` em diante) no Claude Code, na raiz de um diretório vazio.

---

Construa em **Go** a aplicação do **motor de detecção de transações suspeitas**. É um exercício de arquitetura para uma entrevista de engenharia sênior: o foco é qualidade de design, resiliência e testes, não integração real. **Toda entrada e toda consulta externa devem ser mockadas** com implementações em memória — o app roda localmente com um único `go run`, sem Kafka, sem AWS, sem rede.

## Escopo

Implemente **apenas o motor** (não implemente dispatcher, CC, EK7 nem envio de notificação). O motor:

1. Recebe eventos de transação por **duas portas de entrada** que convergem no mesmo núcleo de processamento (`Processor`): (a) uma **fonte mockada** que simula um consumidor Kafka particionado por `customer_id`, e (b) uma **API HTTP** para injetar transações sob demanda e medir desempenho.
2. Verifica **idempotência** (descarta duplicatas).
3. Mantém uma **janela deslizante por cliente** (estado efêmero com TTL) e resolve **parâmetros de risco por cliente** (com default global).
4. Avalia **regras carregadas de configuração** contra três camadas de dado: `event`, `window`, `customer_risk`.
5. **Decide** se a transação é suspeita, agregando severidade e categoria.
6. **Emite** um evento de alerta para um sink mockado (simula publicação no tópico `alertas`).

Princípio inegociável: a API e o consumidor mockado são apenas **adaptadores de entrada**; a lógica de detecção vive uma única vez no `Processor`. Nenhuma regra de negócio pode ser duplicada na camada HTTP.

## Stack e restrições

- Go 1.22+. Prefira a biblioteca padrão. Logging com `log/slog`.
- Bibliotecas de terceiros permitidas e sugeridas: `github.com/expr-lang/expr` para avaliar a `condition` das regras (é o que torna crível "nova regra sem redeploy"), e `github.com/stretchr/testify` para os testes. Não adicione mais nada sem necessidade clara.
- Sem banco, sem SDK de nuvem. Servidor HTTP **é requisito** (porta de entrada + métricas), usando `net/http` padrão — sem framework web.

## Arquitetura

Use **ports & adapters (arquitetura hexagonal)** para que cada dependência externa seja uma interface com um mock em memória plugável. Sugestão de estrutura:

```
cmd/motor/main.go            # wiring por injeção de dependência: sobe API + runner de demonstração
internal/domain/            # entidades e value objects, sem dependências externas
internal/ports/            # interfaces (as "portas"): fontes, stores, config, sink
internal/engine/           # motor de regras: avaliação e agregação
internal/pipeline/         # Processor: orquestra idempotência → janela → regras → decisão → emissão
internal/adapters/memory/  # implementações em memória de todas as portas, com toggles de falha
internal/adapters/httpapi/ # servidor HTTP: adaptador de entrada que chama o Processor
internal/config/           # carga e validação dos perfis de config (embed de JSON)
internal/loadgen/          # gerador de massa sintética de transações (para demo de desempenho)
```

Regras de design: interfaces definidas no lado do consumidor (em `ports`), não no adapter. Construtores explícitos (`NewX(...)`), sem estado global, sem `init()` mágico. `context.Context` como primeiro parâmetro em toda operação que faz I/O ou pode ser cancelada. Erros embrulhados com `%w` e erros-sentinela onde fizer sentido (`ErrDuplicate`, `ErrEnrichmentUnavailable`).

## Modelo de domínio e contratos

Traduza estes contratos para structs de domínio (identificadores e comentários em inglês, seguindo convenção Go). IDs de entidade são UUID (string); `alert_id` é **SHA-256 hex determinístico** sobre serialização fixa e versionada.

**Transação (entrada):**
```json
{
  "customer_id": "uuid", "transaction_id": "uuid",
  "amount": 1899.90, "currency": "BRL", "channel": "pix",
  "device_id": "uuid", "geo": { "country": "BR", "lat": -23.55, "lon": -46.63 },
  "captured_at": "2026-08-24T14:03:11.100Z"
}
```

**Definição de regra (config):**
```json
{
  "rule_id": "velocidade-pix", "version": 4, "enabled": true,
  "requires": ["event", "window"],
  "window": { "span_seconds": 300, "type": "count" },
  "condition": "window.count_channel_pix > 8",
  "emits": { "severity": "alta", "category": "velocidade" }
}
```
`requires` lista as camadas de que a regra depende. `condition` é uma expressão avaliada por `expr` sobre um ambiente montado a partir das camadas disponíveis. `emits` traz a severidade (`baixa|media|alta|critica`) e a categoria.

**Perfil de parâmetros operacionais (config):** inclui o **default global de risco**, usado por todo cliente sem perfil próprio e como fallback quando o store de risco está fora.
```json
{
  "version": 5,
  "default_customer_risk": { "limite_valor": 5000, "nivel": "padrao" },
  "thresholds": { "score_minimo_alerta": 0.6 }
}
```

**Alerta (saída):**
```json
{
  "alert_id": "sha256 hex de v1|customer_id|transaction_id|window_bucket",
  "customer_id": "uuid", "transaction_id": "uuid",
  "severity": "alta", "categories": ["velocidade", "geo"], "score": 0.87,
  "triggered_rules": [{ "rule_id": "velocidade-pix", "version": 4 }],
  "evaluation": "complete | partial",
  "degraded": ["window"]
}
```

## Portas (interfaces a mockar)

- `TransactionSource` — entrega transações (canal/iterador). O mock injeta um roteiro de transações.
- `IdempotencyStore` — `Seen(ctx, key) (bool, error)` + marcação com TTL. Mock em memória com expiração.
- `WindowStore` — grava a transação na janela do cliente e consulta agregados na janela (`CountByChannel`, transações recentes). Mock em memória com TTL e **toggle de indisponibilidade**.
- `RiskStore` — `Get(ctx, customerID) (*RiskProfile, error)`, pode retornar "não encontrado". Mock com alguns clientes personalizados e **toggle de indisponibilidade**.
- `ConfigProvider` — expõe regras + parâmetros da versão ativa, com `Reload()` para simular hot reload. Mock carrega JSON embutido; opcionalmente observa um arquivo.
- `AlertSink` — `Publish(ctx, alert) error`. Mock acumula/loga os alertas emitidos.

Todos os mocks ficam em `internal/adapters/memory` e expõem métodos para simular falha (ex.: `WindowStore.SetDown(true)`), usados pelo runner de demonstração e pelos testes.

## Comportamento do pipeline

Para cada transação:

1. **Idempotência primeiro** (antes de qualquer I/O caro). Chave = `customer_id + transaction_id`. Se já vista, descarta e segue.
2. **Resolver camadas** conforme o `requires` do conjunto de regras: consultar a janela (`window`) e/ou o perfil de risco (`customer_risk`). Sempre atualizar a janela com a transação atual.
3. **Modo degradado**: se uma camada exigida estiver indisponível (store fora), as regras que a exigem são **puladas**, e o alerta resultante (se houver) é marcado `evaluation: "partial"` com a camada em `degraded`. Regras que não exigem a camada continuam avaliando normalmente. `customer_risk` indisponível ou cliente sem perfil ⇒ usar `default_customer_risk`.
4. **Avaliar regras** habilitadas via `expr`, montando o ambiente a partir das camadas disponíveis (ex.: `amount`, `channel`, `window.count_channel_pix`, `risk.limite_valor`).
5. **Agregar**: `severity` = máxima entre as regras disparadas; `categories` = união; `score` = função simples e documentada das severidades (ex.: normalização por peso). Só emite se passar do `score_minimo_alerta`.
6. **Emitir** o alerta pelo `AlertSink`, com `alert_id` determinístico (SHA-256 hex sobre `"v1|"+customer_id+"|"+transaction_id+"|"+window_bucket`).

## Concorrência

O paralelismo é a mesma estratégia nas duas portas de entrada, e o núcleo é **preservar ordem por `customer_id`**. Centralize isso num despachante (`Dispatcher`/`WorkerPool`) que roteia cada transação para um worker pelo **hash do `customer_id`** (`worker = hash(customer_id) % N`): transações do mesmo cliente caem sempre no mesmo worker (ordem garantida), clientes diferentes processam em paralelo. Tanto o consumidor mockado quanto a API alimentam esse mesmo pool — nunca chamam o `Processor` cada um com seu próprio esquema de goroutines.

- **Consumo mockado**: comente no código onde, num sistema real, entraria o commit de offset após o batch (at-least-once).
- **API síncrona** (`POST /transactions`): roteia para o worker do cliente e aguarda o resultado daquele item, devolvendo o veredito no response. Requests de clientes diferentes correm em paralelo.
- **API em lote** (`POST /transactions/batch`): processa o lote em paralelo **repartindo por `customer_id`, não por índice do array** — dois eventos do mesmo cliente no mesmo lote não podem processar fora de ordem. Use um `errgroup` com limite de concorrência ou o próprio pool, e agregue os vereditos.

Número de workers e limite de concorrência do lote são configuráveis (flag/env). Proteja o estado compartilhado dos mocks (mapas de janela/idempotência) com o locking adequado, já que agora há concorrência real — os testes devem rodar com `-race` limpo.

## API HTTP (`internal/adapters/httpapi`)

Servidor `net/http` que é um **adaptador de entrada fino**: valida e desserializa o payload, chama o `Processor` (via pool) e serializa o veredito. Sem lógica de detecção aqui. Endpoints:

- `POST /transactions` — recebe uma transação (mesmo contrato de entrada), processa e responde com o veredito: se gerou alerta, o alerta; se não, um resultado `{ "suspicious": false }`; duplicata ⇒ `{ "status": "duplicate" }`. Use códigos HTTP adequados (200/202, 400 para payload inválido).
- `POST /transactions/batch` — recebe um array de transações, processa em paralelo (respeitando ordem por cliente, ver Concorrência) e responde com o resumo: total, alertas, duplicatas, parciais, e latência agregada (p50/p95/p99, total, throughput em TPS observado).
- `GET /healthz` — liveness.
- `GET /metrics` — snapshot dos contadores em JSON (processadas, duplicatas, alertas, parciais, por categoria, latências). Não precisa ser Prometheus; JSON basta para a demo.
- Opcional, útil na apresentação: `POST /admin/window/down` e `/admin/window/up`, `POST /admin/config/reload` — acionam os toggles de falha e o hot reload por HTTP, para você demonstrar modo degradado e "sem redeploy" ao vivo via `curl`.

Boas práticas: timeouts no `http.Server` (`ReadHeaderTimeout` etc.), `context` do request propagado até o `Processor`, limite de tamanho de payload, e shutdown via `srv.Shutdown(ctx)` no graceful shutdown.

## Gerador de massa (`internal/loadgen`)

Gere massa sintética realista para exercitar o desempenho pela API:

- Função que produz **N transações** (N configurável, com um preset de pelo menos **100.000**) distribuídas entre **M clientes** (ex.: 5.000), com UUIDs válidos, canais variados (`pix`, `ted`, `card`), valores em faixas plausíveis e timestamps crescentes.
- A massa deve **conter gatilhos de propósito**, para os alertas não serem raros na demo: uma fração de clientes com rajada de Pix (dispara `velocidade-pix`), uma fração com valor acima do default (dispara `valor-atipico`), e uma fração de duplicatas (exercita idempotência). Documente as proporções.
- Determinismo opcional por `seed`, para execuções reproduzíveis.
- Exponha isso de duas formas: um subcomando/flag no `cmd/motor` que gera a massa e a envia contra a API medindo throughput e latências (um mini cliente de carga interno, com concorrência configurável), e a possibilidade de dump para arquivo `.jsonl` para uso com `curl`/`vegeta`/`k6` externos.

O objetivo desta parte é permitir, na apresentação, rodar algo como "gere 100k e dispare contra a API" e mostrar TPS observado e p99 no terminal — evidência concreta de que o paralelismo por cliente sustenta a carga. Deixe claro no README que os números refletem o ambiente local e os mocks em memória, não a infraestrutura real.

## Observabilidade e robustez

- `log/slog` estruturado com `trace_id`/`transaction_id` no contexto.
- Contadores em memória expostos ao final da execução: processadas, duplicatas descartadas, alertas emitidos, avaliações parciais, por categoria.
- **Graceful shutdown**: `signal.NotifyContext` com `SIGINT`/`SIGTERM`, drenando o que estiver em voo antes de sair.

## Runner de demonstração e execução (`cmd/motor/main.go`)

O binário deve suportar, por flag/subcomando:

- `serve` (padrão) — sobe a API HTTP e fica no ar; base para a demo via `curl` e para o gerador de carga.
- `demo` — executa um roteiro scriptado que exercita visivelmente cada capacidade, imprimindo os alertas emitidos:
  - Uma transação de valor alto que dispara `valor-atipico` usando o **default** de risco, e a mesma regra para um cliente com **limite personalizado** (não dispara) — mostra `customer_risk`.
  - Uma rajada de transações Pix do mesmo cliente, disparando `velocidade-pix` — mostra a janela.
  - Uma transação **duplicada** — mostra a idempotência.
  - `WindowStore` derrubado (`SetDown(true)`) e uma transação processada — mostra o **modo degradado** (`evaluation: partial`, `degraded: ["window"]`) e a recuperação ao voltar.
  - **Hot reload** de config habilitando uma nova regra, com a próxima transação já avaliada por ela — mostra "sem redeploy".
- `loadtest` — gera a massa sintética (preset de 100k), dispara contra a API com concorrência configurável e imprime **throughput (TPS observado), p50/p95/p99 e contagem de alertas/duplicatas/parciais**. É o número que sustenta a conversa sobre desempenho na entrevista.

## Testes

Cobertura orientada a tabela, com os cenários críticos como casos explícitos:

- Idempotência: mesma transação duas vezes ⇒ processada uma vez.
- `velocidade-pix` dispara após ultrapassar o limite na janela; não dispara abaixo.
- `valor-atipico` com default vs. limite personalizado do cliente.
- Modo degradado: `WindowStore` fora ⇒ regra que exige `window` é pulada, alerta `partial` com `degraded: ["window"]`; regra que só exige `event` segue funcionando.
- `customer_risk` indisponível/cliente sem perfil ⇒ usa `default_customer_risk`.
- Agregação: múltiplas regras ⇒ severidade máxima e união de categorias.
- Determinismo do `alert_id`: mesmas entradas ⇒ mesmo hash; entradas diferentes ⇒ hashes diferentes.
- Config inválida no `ConfigProvider` ⇒ rejeitada, mantendo a última versão válida.
- **Ordem sob concorrência**: um lote com vários eventos do mesmo cliente, processado em paralelo, produz o mesmo resultado da ordem sequencial (a janela não corrompe). Testes devem passar com `go test -race ./...`.
- **API**: teste de handler com `httptest` para `POST /transactions` (veredito correto) e `POST /transactions/batch` (resumo agregado).

## Entregáveis

- Código compilando e testes passando com `go build ./...` e `go test -race ./...`.
- `Makefile` com `run` (sobe a API), `demo`, `loadtest`, `test` e `lint` (se usar `golangci-lint`, deixe opcional).
- `README.md` curto: como rodar cada modo, exemplos de `curl` para os endpoints, o que cada pacote faz, e as decisões de design (ports & adapters, uma única lógica no `Processor` com API e consumo como adaptadores, ordem por `customer_id`, modo degradado via `requires`, regras por config, mocks plugáveis). Deixe explícito que os números de desempenho são locais e sobre mocks em memória.

Comece propondo a estrutura de pacotes e o modelo de domínio, peça confirmação se algo estiver ambíguo, e então implemente em incrementos testáveis (domínio → portas → engine → pipeline → pool/concorrência → adapters memory → API → loadgen → runner), rodando os testes a cada etapa.