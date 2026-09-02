# SDD — Motor de detecção de transações suspeitas

| | |
|---|---|
| **Componente** | Motor de detecção |
| **Linguagem** | Go 1.22+ |
| **Natureza** | Exercício de arquitetura; todas as dependências externas são mockadas em memória |
| **Status** | Baseline para implementação orientada a spec |

Palavras-chave de obrigatoriedade: **DEVE** (requisito), **NÃO DEVE** (proibição), **PODE** (opcional). Cada requisito funcional tem um id `FR-nnn`; não-funcionais, `NFR-nnn`. Critérios de aceite em Dado/Quando/Então são normativos.

---

## 1. Propósito e escopo

O motor identifica transações suspeitas em tempo real: consome eventos de transação, aplica regras de detecção configuráveis contra o evento e o comportamento recente do cliente, e emite alertas. Esta spec cobre o artefato executável de demonstração, com entrada por API HTTP e por uma fonte mockada equivalente a um consumidor Kafka, e todas as consultas de estado mockadas em memória.

**Em escopo:** ingestão dupla (API + consumo mockado), idempotência, janela deslizante por cliente, perfil de risco por cliente com default, motor de regras por configuração, decisão e agregação, emissão de alerta, modo degradado, hot reload de configuração, paralelismo preservando ordem por cliente, geração de massa e teste de carga local, observabilidade e testes.

## 2. Glossário

**Camada de dado**: origem de informação que uma regra pode consumir — `event` (a própria transação), `window` (comportamento recente do cliente) ou `customer_risk` (parâmetros de risco por cliente). **Modo degradado**: operação com uma ou mais camadas indisponíveis, pulando as regras que as exigem. **Processor**: núcleo único que orquestra o pipeline; as portas de entrada são adaptadores finos sobre ele. **Alerta parcial**: alerta emitido com pelo menos uma camada exigida ausente.

## 3. Visão geral da arquitetura

O sistema segue **ports & adapters**. O `Processor` contém toda a lógica de detecção e depende apenas de interfaces (portas). Entradas (API HTTP, consumidor mockado) e saídas/estado (idempotência, janela, risco, config, sink de alerta) são adaptadores plugáveis. Nesta entrega todos os adaptadores de estado são implementações em memória. O paralelismo é centralizado num pool que roteia por `customer_id`, compartilhado pelas duas portas de entrada.

## 4. Requisitos funcionais

### FR-001 — Ingestão por duas portas convergentes
O sistema DEVE aceitar transações por (a) uma API HTTP e (b) uma fonte mockada que simula um consumidor Kafka particionado por `customer_id`. Ambas DEVEM invocar o mesmo `Processor`. Nenhuma lógica de detecção DEVE existir nos adaptadores de entrada.
- *Dado* o motor no ar, *Quando* a mesma transação é submetida pela API e pela fonte mockada, *Então* o veredito produzido é idêntico.

### FR-002 — Idempotência
O sistema DEVE descartar transações já processadas, identificadas por `customer_id + transaction_id`, antes de qualquer consulta de enriquecimento. Uma duplicata NÃO DEVE gerar segundo alerta nem alterar a janela.
- *Dado* uma transação já processada, *Quando* ela é submetida novamente, *Então* o resultado é `duplicate` e nenhum alerta adicional é emitido nem a janela é alterada.

### FR-003 — Janela deslizante por cliente
O sistema DEVE manter, por cliente, uma janela do comportamento recente com expiração por TTL, e DEVE atualizá-la com a transação corrente. A janela DEVE suportar, no mínimo, contagem por canal em um intervalo (`span_seconds`).
- *Dado* N transações Pix de um cliente dentro de `span_seconds`, *Quando* a contagem por canal é consultada, *Então* o valor retornado é N.
- *Dado* transações fora do intervalo, *Quando* a janela é consultada, *Então* elas não são contabilizadas.

### FR-004 — Perfil de risco por cliente com default
O sistema DEVE resolver parâmetros de risco por cliente. Se o cliente não tiver perfil, ou se o store de risco estiver indisponível, o sistema DEVE usar o `default_customer_risk` da configuração. O default NÃO DEVE residir por cliente no store.
- *Dado* um cliente sem perfil de risco, *Quando* uma regra usa `risk.limite_valor`, *Então* o valor aplicado é o do default de configuração.
- *Dado* um cliente com perfil personalizado, *Quando* a mesma regra é avaliada, *Então* o valor aplicado é o do perfil do cliente.

### FR-005 — Motor de regras por configuração
As regras DEVEM ser carregadas de configuração, cada uma declarando `requires` (camadas de que depende), `condition` (expressão) e `emits` (`severity`, `category`). A `condition` DEVE ser avaliada dinamicamente (via `expr-lang/expr`) sobre um ambiente montado a partir das camadas disponíveis, de modo que uma nova regra seja habilitável sem recompilar.
- *Dado* um conjunto de regras em configuração, *Quando* uma transação satisfaz a `condition` de uma regra habilitada, *Então* essa regra dispara e contribui com sua `severity` e `category`.
- *Dado* uma regra desabilitada (`enabled: false`), *Quando* a transação a satisfaria, *Então* ela não dispara.

### FR-006 — Decisão e agregação
Quando uma ou mais regras disparam, o sistema DEVE agregar: `severity` = máxima entre as regras disparadas; `categories` = união das categorias; `score` = função documentada e determinística das severidades. O alerta só DEVE ser emitido se `score >= score_minimo_alerta`.
- *Dado* duas regras disparadas com severidades `media` e `alta`, *Quando* o resultado é agregado, *Então* a `severity` é `alta` e as duas categorias constam em `categories`.
- *Dado* um `score` abaixo do mínimo configurado, *Quando* a decisão é tomada, *Então* nenhum alerta é emitido.

### FR-007 — Emissão de alerta com identificador determinístico
O sistema DEVE emitir o alerta por um sink (porta de saída) e o `alert_id` DEVE ser o SHA-256 hex de uma serialização fixa e versionada `"v1|"+customer_id+"|"+transaction_id+"|"+window_bucket`. IDs de entidade permanecem UUID; o `alert_id` é hash, não UUID.
- *Dado* as mesmas entradas, *Quando* o alerta é gerado duas vezes, *Então* o `alert_id` é idêntico.
- *Dado* entradas diferentes, *Quando* os alertas são gerados, *Então* os `alert_id` diferem.

### FR-008 — Modo degradado por camada
Se uma camada exigida por uma regra estiver indisponível, o sistema DEVE pular as regras que a exigem e DEVE continuar avaliando as demais. Um alerta emitido nessas condições DEVE ter `evaluation: "partial"` e listar a camada ausente em `degraded`. O sistema NÃO DEVE falhar a transação por indisponibilidade de camada.
- *Dado* o store de janela indisponível, *Quando* uma transação é processada, *Então* regras que exigem `window` são puladas, regras que exigem apenas `event` seguem avaliando, e qualquer alerta sai `partial` com `degraded: ["window"]`.
- *Dado* o store de janela restaurado, *Quando* a próxima transação é processada, *Então* a avaliação volta a `complete`.

### FR-009 — Hot reload de configuração
O sistema DEVE permitir recarregar a configuração de regras e parâmetros em runtime, sem reiniciar. Uma configuração inválida DEVE ser rejeitada, mantendo ativa a última versão válida.
- *Dado* o motor no ar, *Quando* uma nova versão de configuração habilita uma regra, *Então* a próxima transação é avaliada por ela sem reinício.
- *Dado* uma configuração inválida submetida ao reload, *Quando* o reload é solicitado, *Então* ele é rejeitado e a versão anterior permanece ativa.

### FR-010 — Ordem por cliente sob concorrência
O processamento DEVE ser paralelo entre clientes e DEVE preservar a ordem por `customer_id`. Transações do mesmo cliente DEVEM ser processadas na ordem de chegada, inclusive em requisições em lote processadas em paralelo (repartição por `customer_id`, não por índice).
- *Dado* um lote com múltiplas transações do mesmo cliente processado em paralelo, *Quando* o processamento conclui, *Então* o resultado é igual ao do processamento sequencial e a janela não fica inconsistente.

### FR-011 — API HTTP
O sistema DEVE expor: `POST /transactions` (uma transação; responde o veredito), `POST /transactions/batch` (array; responde resumo com totais e latências agregadas p50/p95/p99 e TPS observado), `GET /healthz` (liveness) e `GET /metrics` (snapshot de contadores em JSON). Payload inválido DEVE retornar 4xx; a lógica DEVE residir no `Processor`, não no handler.
- *Dado* uma transação suspeita via `POST /transactions`, *Quando* processada, *Então* a resposta contém o alerta; se não suspeita, `{ "suspicious": false }`; se duplicata, `{ "status": "duplicate" }`.
- *Dado* um payload malformado, *Quando* enviado, *Então* a resposta é 400 e nada é processado.

### FR-012 — Geração de massa e teste de carga
O sistema DEVE gerar massa sintética (preset mínimo de 100.000 transações, M clientes configurável), contendo gatilhos plantados em proporções documentadas (rajadas de Pix, valores acima do default, duplicatas). DEVE haver um modo `loadtest` que dispara a massa contra a API com concorrência configurável e reporta throughput e latências. Geração DEVE ser reproduzível por `seed`.
- *Dado* o preset de 100k, *Quando* o `loadtest` roda, *Então* o relatório inclui TPS observado, p50/p95/p99 e contagens de alertas, duplicatas e parciais, e alertas não são nulos.

### FR-013 — Controles administrativos para demonstração
O sistema PODE expor endpoints administrativos para acionar os toggles de falha e o hot reload por HTTP (`/admin/window/down`, `/admin/window/up`, `/admin/config/reload`), destinados à demonstração ao vivo de FR-008 e FR-009.

## 5. Requisitos não-funcionais

### NFR-001 — Orçamento de latência (local)
O caminho por transação (idempotência → resolução de camadas → avaliação → emissão) DEVE ser projetado para caber em um orçamento de baixa latência. Métricas de latência DEVEM ser mensuráveis via `loadtest`. Números reportados referem-se ao ambiente local e a mocks em memória, e NÃO DEVEM ser apresentados como capacidade da infraestrutura real.

### NFR-002 — Paralelismo
O paralelismo entre clientes DEVE ser configurável (número de workers e limite de concorrência do lote). O throughput observado DEVE escalar com o paralelismo até o limite de contenção dos mocks.

### NFR-003 — Segurança de concorrência
Todo estado compartilhado nos adaptadores em memória DEVE ser protegido contra corrida. A suíte DEVE passar com `go test -race ./...` sem avisos.

### NFR-004 — Autossuficiência
O sistema DEVE executar sem rede, broker ou nuvem: `go run` sobe tudo. Nenhuma dependência externa de runtime DEVE ser exigida além das bibliotecas Go declaradas.

### NFR-005 — Observabilidade
O sistema DEVE emitir logs estruturados (`log/slog`) com correlação por `trace_id`/`transaction_id` e DEVE expor contadores (processadas, duplicatas, alertas, parciais, por categoria, latências) via `GET /metrics` e ao fim das execuções scriptadas.

### NFR-006 — Encerramento gracioso
O sistema DEVE tratar `SIGINT`/`SIGTERM` via `signal.NotifyContext`, drenar o trabalho em voo e desligar o servidor HTTP com `Shutdown(ctx)` antes de sair.

### NFR-007 — Qualidade de código
O código DEVE seguir ports & adapters com interfaces definidas no lado do consumidor, construtores explícitos, ausência de estado global, `context.Context` como primeiro parâmetro em operações canceláveis/de I/O, e erros embrulhados com `%w` e erros-sentinela (`ErrDuplicate`, `ErrLayerUnavailable`).

### NFR-008 — Robustez da API
O `http.Server` DEVE definir timeouts (incl. `ReadHeaderTimeout`), limitar o tamanho de payload e propagar o `context` do request até o `Processor`.

## 6. Modelo de domínio e contratos

Identificadores de entidade são UUID (string). Serialização JSON com `snake_case`. Estruturas de domínio residem em `internal/domain` sem dependências externas.

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
`severity` ∈ {`baixa`,`media`,`alta`,`critica`}. `requires` ⊆ {`event`,`window`,`customer_risk`}.

**Parâmetros operacionais (config):**
```json
{
  "version": 5,
  "default_customer_risk": { "limite_valor": 5000, "nivel": "padrao" },
  "thresholds": { "score_minimo_alerta": 0.6 }
}
```

**Perfil de risco por cliente (store, opcional por cliente):**
```json
{ "customer_id": "uuid", "limite_valor": 15000, "nivel": "private" }
```

**Alerta (saída):**
```json
{
  "alert_id": "sha256-hex", "customer_id": "uuid", "transaction_id": "uuid",
  "severity": "alta", "categories": ["velocidade", "geo"], "score": 0.87,
  "triggered_rules": [{ "rule_id": "velocidade-pix", "version": 4 }],
  "evaluation": "complete | partial", "degraded": ["window"]
}
```

## 7. Portas (contratos de interface)

Assinaturas conceituais; `context.Context` é o primeiro parâmetro. Definidas em `internal/ports`.

- `TransactionSource` — fornece transações ao pool (canal ou iterador). Mock injeta um roteiro.
- `IdempotencyStore` — `SeenAndMark(ctx, key) (already bool, err error)` com TTL; operação atômica de verificar-e-marcar.
- `WindowStore` — `Append(ctx, tx) error`; `CountByChannel(ctx, customerID, channel, span) (int, error)`; `Recent(ctx, customerID, span) ([]TxSummary, error)`. Expõe toggle de indisponibilidade para teste/demo.
- `RiskStore` — `Get(ctx, customerID) (*RiskProfile, found bool, err error)`. Expõe toggle de indisponibilidade.
- `ConfigProvider` — `Current() Config`; `Reload(source) error` (valida antes de trocar). Config inclui regras + parâmetros com versão.
- `AlertSink` — `Publish(ctx, alert) error`. Mock acumula e loga.

Contratos de erro: indisponibilidade DEVE ser sinalizada por erro-sentinela distinguível (`ErrLayerUnavailable`) para que o pipeline aplique FR-008; "não encontrado" NÃO é erro (é `found=false`).

## 8. Especificação do pipeline (Processor)

Para cada transação, na ordem:

1. **Idempotência.** Chamar `IdempotencyStore.SeenAndMark`. Se `already`, encerrar com resultado `duplicate` (FR-002). Esta etapa precede qualquer consulta de camada.
2. **Determinar camadas exigidas.** União dos `requires` das regras habilitadas.
3. **Resolver camadas.** Sempre `Append` a transação à janela. Para `window`/`customer_risk` exigidas, consultar os stores; indisponibilidade marca a camada como degradada (FR-008); `customer_risk` ausente/indisponível resolve para o default (FR-004).
4. **Avaliar regras** habilitadas cujas camadas exigidas estejam disponíveis, montando o ambiente `expr` a partir das camadas resolvidas (FR-005).
5. **Agregar** severidade, categorias e score; aplicar `score_minimo_alerta` (FR-006).
6. **Emitir** alerta via `AlertSink` com `alert_id` determinístico e marcação `evaluation`/`degraded` (FR-007, FR-008).

Pós-condições: a janela reflete a transação corrente exatamente uma vez; nenhum alerta duplicado para a mesma transação; contadores de observabilidade atualizados.

## 9. Concorrência

Um pool (`WorkerPool`) roteia cada transação para um worker por `worker = hash(customer_id) % N`, garantindo serialização por cliente e paralelismo entre clientes (FR-010). As duas portas de entrada usam esse pool. Requisições em lote repartem por `customer_id` e agregam vereditos, respeitando a serialização por cliente. O commit de offset (consumo real) é comentado como ponto de extensão at-least-once.

## 10. Configuração e validação

Configuração carregada de JSON embutido (`embed`) e recarregável (FR-009). A validação DEVE cobrir: `severity`/`category` em enums conhecidos; `requires` ⊆ camadas suportadas; `condition` compilável pelo avaliador; campos obrigatórios presentes; versão monotônica. Validação falha ⇒ reload rejeitado, versão anterior mantida.

## 11. Plano de testes (rastreável a requisitos)

Testes orientados a tabela, executados com `-race` (NFR-003). Cobertura mínima:

- FR-002: duplicata processada uma vez.
- FR-003: contagem dentro/fora do intervalo da janela.
- FR-004: default vs. perfil personalizado.
- FR-005: regra dispara/não dispara; regra desabilitada não dispara; regra nova via config passa a disparar.
- FR-006: agregação de severidade máxima e união de categorias; corte por score mínimo.
- FR-007: determinismo e unicidade do `alert_id`.
- FR-008: janela fora ⇒ regra de `window` pulada, `event` segue, alerta `partial`; recuperação ⇒ `complete`.
- FR-009: reload válido aplica; reload inválido rejeitado mantendo versão.
- FR-010: lote concorrente do mesmo cliente = resultado sequencial; `-race` limpo.
- FR-011: handlers de `POST /transactions` e `/transactions/batch` via `httptest`.
- FR-012: `loadtest` do preset produz relatório com métricas e alertas não nulos.

Um requisito é considerado atendido somente quando existe teste verde correspondente ao seu critério de aceite.

## 12. Estrutura de pacotes

```
cmd/motor/main.go            # wiring + subcomandos: serve | demo | loadtest
internal/domain/            # entidades e value objects
internal/ports/            # interfaces (portas)
internal/engine/           # avaliação de regras e agregação
internal/pipeline/         # Processor (orquestração)
internal/adapters/memory/  # stores/sink/config/source em memória, com toggles
internal/adapters/httpapi/ # servidor HTTP (adaptador de entrada + métricas)
internal/config/           # carga e validação dos perfis (embed)
internal/loadgen/          # geração de massa e cliente de carga
```

## 13. Plano de implementação (fases)

1. Domínio e contratos (§6) + portas (§7).
2. Engine de regras (§8 passos 4–5) com testes de FR-005/FR-006.
3. Processor completo (§8) com FR-002/FR-003/FR-004/FR-007/FR-008.
4. Pool de concorrência (§9) com FR-010 e `-race`.
5. Adaptadores em memória com toggles.
6. Config + hot reload (§10) com FR-009.
7. API HTTP (§7/FR-011) e admin (FR-013).
8. `loadgen` + `loadtest` (FR-012).
9. Runner `demo`, README, Makefile.

Cada fase entrega testes verdes antes da seguinte.

## 14. Premissas e questões em aberto

Premissas: a janela em memória é aceitável como estado efêmero para a demo (reset no reinício é tolerável); o avaliador de expressão cobre as condições previstas sem necessidade de DSL própria; os números de desempenho são locais e ilustrativos. Questões em aberto para discussão na apresentação: estratégia de reidratação da janela num sistema real (reset tolerável vs. externalizar estado vs. replay de offset); e se `customer_risk` justifica cache local com invalidação, como a janela.