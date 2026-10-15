# Manifesto — vip-ia

> Serviço de Inteligência Artificial da plataforma VIP PABX. Quinto app do workspace.
> Data: 2026-10-14 (revisado em 2026-10-15) · Status: design aprovado, implementação não iniciada.

## 1. Por que existe

Toda chamada gravada da plataforma vira um `.mp3` no S3 que ninguém escuta. O `vip-ia` transforma essas gravações
em **texto pesquisável** e, sobre o texto, em **inteligência**: classificações, respostas, resumos e análises
geradas por agentes de IA que o próprio cliente escolhe (ou cria) e aplica às suas chamadas.

É um **módulo pago**: só empresas que contrataram o módulo de IA têm chamadas transcritas. Quem decide isso é o
`vip-pabx-manager`, não o `vip-ia`.

## 2. Escopo em duas fases

### Fase 1 — Transcrição

1. O manager recebe a CDR do vip-worker (fluxo atual) e, **se a empresa tem o módulo de IA**, republica a CDR no
   tópico `KAFKA_MANAGER_IA_CDR_TOPIC`.
2. O `vip-ia` consome esse tópico e cria uma tarefa de transcrição por gravação em `recordNames`.
3. Baixa a gravação do S3 (`recordings/{companyId}/{recordName}.mp3`, bucket de gravações do vip-worker).
4. Transcreve com a **Deepgram** (SDK Java) e grava texto + falas por locutor no MySQL próprio.
5. Registra o consumo (segundos de áudio) para faturamento.
6. Expõe as transcrições ao frontend via API REST própria.

### Fase 2 — Agentes de IA

1. Agentes são prompts configurados com modelo (via **OpenRouter**) e **tipo de saída**.
2. Existe um **catálogo global** (agentes da plataforma + agentes públicos de empresas) e **agentes próprios** de
   cada empresa.
3. Agentes podem rodar **automaticamente** em toda transcrição nova da empresa e/ou **sob demanda** sobre uma
   chamada ou um lote de chamadas já transcritas.
4. Resultados ficam vinculados à transcrição e são consultáveis/agregáveis pela API.
5. Tokens e custo de cada execução são registrados para faturamento.

## 3. Decisões tomadas

| Tema | Decisão |
|------|---------|
| Stack | Kotlin 2.3.21, **Spring Boot 4.1.x** (mesmas versões do vip-worker), JDK 21 (virtual threads), Spring Data JDBC, MySQL 8 próprio |
| Transcrição | **Deepgram Java SDK 0.11.1** — `transcribeFile(MediaTranscribeRequestOctetStream)` com `nova-3`, `pt-BR`, `diarize`, `utterances`, `punctuate`, `smart_format` |
| LLM | **Spring AI 2.x** (`spring-ai-starter-model-openai`) com `base-url` do OpenRouter; modelo definido por agente |
| Entrada | Tópico **novo publicado pelo manager** (`KAFKA_MANAGER_IA_CDR_TOPIC`); o vip-ia **não** consome `KAFKA_WORKER_CDR_TOPIC` |
| API | REST própria na porta **8083**, validando o **JWT do manager** (`HMAC512(AUTH_JWT_SECRET)`, issuer `"VIP API"`) |
| Agentes | Catálogo global + agentes por empresa; agente de empresa é `PRIVATE` por padrão e pode virar `PUBLIC` |
| Execução | Automática (por assinatura da empresa) **e** sob demanda (chamada única ou lote) |
| Saída | Definida pelo agente: `TEXT`, `CLASSIFICATION` (labels fixos) ou `JSON` (schema) |
| Consumo | Registrado em `usage_record` no vip-ia; **não** publicado no Kafka. O relatório financeiro consulta o vip-ia |
| Processamento | Fila de tarefas no banco + processadores agendados (listener Kafka só persiste) |

**Alternativas registradas, não adotadas agora:** consumir direto o tópico de CDR do worker (descartado: o manager
é quem sabe se a empresa paga o módulo); `transcribeUrl` com URL pré-assinada do S3 em vez de baixar os bytes
(válido se o tráfego de download virar problema); proxy via manager (descartado: acoplamento e volume no manager).

## 4. Arquitetura

```
vip-worker ──CDR──> vip-pabx-manager ──(empresa tem módulo IA?)──> KAFKA_MANAGER_IA_CDR_TOPIC
                                                                         │
                                                                         ▼
┌────────────────────────────── vip-ia (:8083) ──────────────────────────────────────┐
│ IaCdrListener ──> transcription (PENDING) ──commit offset                           │
│                                                                                     │
│ TranscriptionProcessor (@Scheduled, virtual threads, concorrência limitada)         │
│   RecordingStorage (S3) → TranscriptionProvider (Deepgram) → transcription DONE     │
│   → transcription_utterance → usage_record → agent_run PENDING (agentes automáticos)│
│                                                                                     │
│ AgentRunProcessor (fase 2)                                                          │
│   prompt do agente + transcrição por locutor → ChatClient (OpenRouter)              │
│   → valida saída (labels/schema) → agent_run DONE → usage_record                    │
│                                                                                     │
│ API REST (JWT do manager) ◄──────────────────────────────────── frontend (Angular)  │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Por que fila no banco e não processar no listener

- A gravação **ainda não está no S3** quando a CDR chega: o vip-worker publica a CDR e só depois converte
  `.sln → .mp3` e sobe (`CdrService.sendCdr` → `RecordConversionService`, numa única virtual thread serial).
  Esperar dentro do listener travaria a partição.
- Falhas da Deepgram/OpenRouter não perdem a mensagem; restart não perde trabalho.
- Controle explícito de concorrência = controle de custo e de rate limit.

### Unidades e interfaces

| Unidade | Responsabilidade | Depende de |
|---------|------------------|------------|
| `IaCdrListener` | Desserializa a mensagem, cria tarefas idempotentes | `TranscriptionRepository` |
| `RecordingStorage` | `fun download(companyId, file): ByteArray` | AWS SDK S3 |
| `TranscriptionProvider` | `fun transcribe(audio): TranscriptionResult` (texto, falas, duração, request id) | Deepgram SDK |
| `TranscriptionProcessor` | Seleciona tarefas `PENDING`/`RETRY` vencidas, orquestra, aplica retry/backoff | as três acima |
| `AgentExecutor` (fase 2) | Monta prompt, chama o modelo, valida a saída conforme `output_type` | Spring AI `ChatClient` |
| `AgentRunProcessor` (fase 2) | Seleciona `agent_run` pendentes, orquestra, aplica retry | `AgentExecutor` |
| `UsageService` | Grava `usage_record` | repositório |

## 5. Contrato da mensagem (manager → vip-ia)

Tópico `KAFKA_MANAGER_IA_CDR_TOPIC`, JSON:

```json
{
  "companyId": "100023",
  "uniqueId": "1772633126.1880",
  "startTime": "2026-03-04T11:05:26-03:00",
  "duration": 8,
  "billableSeconds": 2,
  "src": "12_100023",
  "destination": "1132931515",
  "peer": "12",
  "disposition": "ANSWERED",
  "userfield": "OUTBOUND",
  "recordNames": ["100023-1772633126.1880-MIXED"]
}
```

- `startTime` em **epoch millis** (mesmo formato do `Date` serializado no tópico de CDR do worker).
- `recordNames`: os `mixedRecord` (sem extensão) de `channelInteractions` da CDR. **Atenção:** o campo
  `callRecords` da CDR do worker existe mas vem sempre vazio — as gravações reais estão em
  `channelInteractions[].mixedRecord`. O vip-ia monta a chave `recordings/{companyId}/{recordName}.mp3`.
- O golden file fica em `src/test/resources/kafka-contracts/ia-cdr-message.json` nos **dois** lados (manager e vip-ia).
- CDR sem `recordNames` → nenhuma tarefa é criada (só log).
- **Pendência no manager:** criar a flag de módulo de IA por empresa, o novo tópico e o produtor da mensagem.

## 6. Modelo de dados

### Fase 1

**`transcription`**
- `id`, `company_id`, `cdr_unique_id`, `record_name`
- `status` (`PENDING`, `PROCESSING`, `RETRY`, `DONE`, `FAILED`), `attempts`, `next_attempt_at`, `last_error`
- Metadados da CDR: `call_start_time`, `src`, `destination`, `peer`, `disposition`, `userfield`, `call_duration`
- Resultado: `audio_seconds`, `language`, `model`, `deepgram_request_id`, `transcript` (TEXT)
- `created_at`, `updated_at`
- **Única:** `(company_id, cdr_unique_id, record_name)`; índices por `(company_id, call_start_time)` e `(status, next_attempt_at)`

**`transcription_utterance`**
- `id`, `transcription_id`, `speaker`, `start_seconds`, `end_seconds`, `text`, `confidence`

**`usage_record`**
- `id`, `company_id`, `type` (`TRANSCRIPTION`, `AGENT_RUN`), `reference_id`, `provider` (`DEEPGRAM`, `OPENROUTER`),
  `model`, `audio_seconds`, `input_tokens`, `output_tokens`, `cost` (nullable), `created_at`
- Índice por `(company_id, created_at)`
- A Deepgram não devolve custo na resposta: em transcrições `cost` fica NULL e o relatório financeiro aplica o
  preço por minuto sobre `audio_seconds`. No OpenRouter o custo vem na resposta e é gravado.

### Fase 2

**`agent`**
- `id`, `owner_company_id` (NULL = plataforma), `name`, `description`, `prompt`, `model` (id OpenRouter, ex.: `openai/gpt-4o-mini`)
- `output_type` (`TEXT`, `CLASSIFICATION`, `JSON`), `output_labels` (JSON), `output_schema` (JSON Schema)
- `visibility` (`PRIVATE`, `PUBLIC`), `version`, `active`, `created_at`, `updated_at`

**`company_auto_agent`**
- `company_id`, `agent_id` — agentes que rodam automaticamente nas novas transcrições da empresa

**`agent_run`**
- `id`, `company_id`, `agent_id`, `agent_version`, `transcription_id`, `trigger` (`AUTO`, `ON_DEMAND`)
- `status`, `attempts`, `next_attempt_at`, `last_error`
- `result_text`, `result_json`, `result_labels`
- `model`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `finished_at`

### Regras de agentes

- Só a empresa dona edita um agente; cada edição incrementa `version` (resultados antigos ficam rastreáveis).
- Agentes da plataforma: gerenciados por `ROLE_SAAS_ADMIN`. Agentes da empresa: `ROLE_COMPANY_ADMIN`.
- Uma empresa pode usar (sob demanda ou no automático) qualquer agente `PUBLIC`, mas não editá-lo.
- Se um agente voltar a `PRIVATE` ou for desativado, outras empresas deixam de executá-lo; resultados antigos permanecem.
- O `agent_run` pertence à empresa que executou (`company_id`), nunca ao dono do agente.

## 7. API REST (porta 8083)

Todas as rotas exigem o JWT do manager e filtram por `companyId` do token.

| Método | Rota | Fase | Descrição |
|--------|------|------|-----------|
| GET | `/transcriptions` | 1 | Lista paginada; filtros: período, `src`, `destination`, `peer`, `status` |
| GET | `/transcriptions/{id}` | 1 | Texto, falas e execuções de agentes |
| GET | `/transcriptions/by-cdr/{uniqueId}` | 1 | Transcrições de uma CDR (detalhe de CDR do frontend) |
| POST | `/transcriptions/{id}/retry` | 1 | Reprocessa uma transcrição `FAILED` |
| GET | `/internal/usage?companyId&from&to` | 1 | Consumo para o relatório financeiro (`ROLE_SAAS_FINANCE`+) |
| GET/POST/PUT/DELETE | `/agents` | 2 | CRUD; `GET` lista próprios + `PUBLIC` |
| PUT/DELETE | `/agents/{id}/auto` | 2 | Liga/desliga o agente no automático da empresa |
| POST | `/agents/{id}/runs` | 2 | Execução sob demanda: lista de `transcriptionIds` ou filtro de período |
| GET | `/agent-runs` | 2 | Resultados; filtros por agente, label, período (base para dashboards) |
| GET | `/actuator/health`, `/actuator/prometheus` | 1 | Sem token; proteção de rede |

## 8. Tratamento de erros

| Situação | Ação |
|----------|------|
| Gravação ainda não existe no S3 | `RETRY` com backoff exponencial (limite configurável; padrão 6 tentativas com base 1min ≈ 31min) → `FAILED` |
| Deepgram/OpenRouter 5xx, 429, timeout | `RETRY` com backoff |
| Deepgram/OpenRouter 4xx de requisição | `FAILED` com `last_error` |
| Saída do agente fora dos labels/schema | Conta como falha → `RETRY` → `FAILED` |
| Mensagem Kafka malformada | Log e descarte; não trava o consumo |
| Tarefa presa em `PROCESSING` (restart) | Ao subir, volta para `RETRY` |
| CDR duplicada | Ignorada pela chave única |

## 9. Testes

- H2 (modo MySQL) com `schema.sql` mantido à mão; Kafka excluído.
- Golden file do contrato da mensagem do manager.
- Fakes para `RecordingStorage`, `TranscriptionProvider` e `ChatModel` — nunca chamar APIs pagas nos testes.
- Processadores: transições de estado, backoff, limite de tentativas, recuperação de `PROCESSING`.
- Segurança: sem token, token inválido, isolamento por `companyId`, visibilidade `PRIVATE`/`PUBLIC`, roles.
- Fase 2: validação de saída por `output_type`.

## 10. Fora de escopo (por ora)

- Telas no frontend (spec própria quando a API da fase 1 estiver pronta).
- Busca full-text avançada sobre transcrições.
- Transcrição em tempo real (streaming).
- Política de retenção/expurgo de transcrições.
- Publicação de consumo no Kafka.

## 11. Riscos e pontos de atenção

- **Spring Boot 4.1 / Jackson 3 / Kotlin 2.3** alinham com o vip-worker, mas divergem do manager e do register
  (Boot 3.5, Kotlin 1.9.25, Jackson 2): código desses dois não é diretamente reaproveitável.
- **Custo:** transcrição de todas as chamadas de empresas com módulo ativo + agentes automáticos. Concorrência
  limitada e `usage_record` desde o primeiro dia.
- **LGPD:** transcrições contêm dados pessoais. Acesso sempre restrito por `companyId`; retenção a definir.
- **Dependência do manager:** a fase 1 só recebe dados depois que o manager publicar no novo tópico.
