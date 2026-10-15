# AGENTS.md

Este arquivo orienta agentes de IA (Claude Code, OpenCode etc.) que trabalham neste repositório.

> Documentos irmãos: `MANIFESTO.md` (visão, escopo e design do serviço) e o `AGENTS.md` da raiz do workspace
> (regras de fluxo de trabalho comuns a todos os apps). Em caso de conflito, as regras da raiz prevalecem.

## Regras de fluxo de trabalho

- Comunique-se sempre em **português do Brasil**. Comentários e logs também em pt-BR, no estilo do código existente.
- Trabalhe sempre na branch **`dev`**. **Nunca** faça commit, implementação ou merge direto em `main`/`master`.
- Use o **Context7 MCP** antes de escrever código com Spring Boot 4, Spring AI 2, Kotlin, Deepgram Java SDK,
  AWS SDK (S3), Spring Kafka e Docker. O **vip-worker** já está em Spring Boot 4.1 / Kotlin 2.3 e é a referência
  de padrões; o manager e o register ainda estão em Boot 3.5 / Kotlin 1.9 (Jackson 2) — **não copie código deles
  sem adaptar**.
- Rode os testes com **`./tests.sh`** (carrega o `.env`), nunca `./gradlew test` puro, salvo teste único (abaixo).

## Contexto

`vip-ia` é o **quinto serviço** da plataforma PABX do workspace `~/Workspaces/vip-workspace`: o serviço de IA.

| Serviço              | Stack                    | Porta | Papel                                                              |
|----------------------|--------------------------|-------|--------------------------------------------------------------------|
| **vip-pabx-manager** | Kotlin/Spring Boot 3.5   | 8080  | API core: CRUDs, tarifação de CDR, autenticação (emite o JWT)      |
| **vip-register**     | Kotlin/Spring Boot 3.5   | 8081  | Configuração do Asterisk, AMI, anti-invasão                        |
| **vip-worker**       | Kotlin/Spring Boot 4.1   | 8082  | Chamadas em tempo real via ARI; grava e sobe as gravações no S3    |
| **vip-ia**           | Kotlin/Spring Boot 4.1   | 8083  | Transcrição de chamadas (Deepgram) e agentes de IA (OpenRouter)    |
| **frontend**         | Angular 19               | 4200  | UI: relatórios, WebPhone (JsSIP), gestão de usuários               |

Resumo do que o serviço faz (detalhes em `MANIFESTO.md`):

- **Fase 1 — Transcrição:** consome as CDRs que o **manager** publica num tópico dedicado (só de empresas que
  contrataram o módulo de IA), baixa as gravações do S3, transcreve com a **Deepgram** e guarda no MySQL próprio.
- **Fase 2 — Agentes:** roda agentes (prompts) via **OpenRouter** sobre as transcrições, de forma automática ou
  sob demanda, gerando texto, classificações ou JSON estruturado.

## Stack

- **Kotlin 2.3.21**, **JDK 21** com **virtual threads** (`spring.threads.virtual.enabled=true`) — mesmas versões do vip-worker.
- **Spring Boot 4.1.x** — Jackson 3 (`tools.jackson.*`) é o padrão; configure o compilador Kotlin com
  `-Xannotation-default-target=param-property` (recomendação da doc do Boot 4).
- **Spring AI 2.x** (fase 2) — `spring-ai-starter-model-openai` apontado para o OpenRouter via
  `spring.ai.openai.base-url=https://openrouter.ai/api`; o modelo de cada agente é passado por `OpenAiChatOptions`.
- **Deepgram Java SDK** (`com.deepgram:deepgram-java-sdk:0.11.1`) — `client.listen().v1().media().transcribeFile(request)`.
  O SDK usa Jackson 2 internamente (convive com o Jackson 3 do Boot); não misture os dois no código do app.
- **AWS SDK v2 (S3)** — leitura do bucket de gravações (`recordings/{companyId}/{arquivo}.mp3`).
- **Spring Kafka**, **Spring Data JDBC** (não JPA), **MySQL 8** em produção, **H2** (modo MySQL) nos testes.
- **Spring Security** + `com.auth0:java-jwt` para validar o JWT emitido pelo manager.

## Comandos

Os scripts `.sh` exportam o `.env` da raiz do app e **abortam com exit 1 se `.env` não existir**.

```bash
./build.sh               # .env + ./gradlew clean build
./run.sh                 # .env + ./gradlew bootRun   (app em :8083)
./tests.sh               # .env + ./gradlew test      (ATENÇÃO: "tests.sh", não "test.sh")
./deploy-production.sh   # deploy para o serviço systemd remoto
```

Teste único — o profile de teste usa valores fixos, então `.env` não é necessário:

```bash
./gradlew test --tests "br.com.vipsolutions.vipia.transcription.TranscriptionServiceTest"
```

## Variáveis de ambiente (`.env`)

| Variável                          | Uso                                                            |
|-----------------------------------|----------------------------------------------------------------|
| `SERVER_PORT`                     | Porta HTTP (padrão 8083)                                       |
| `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` | MySQL próprio do vip-ia                                 |
| `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | Kafka da plataforma    |
| `KAFKA_MANAGER_IA_CDR_TOPIC`      | Tópico onde o manager publica as CDRs com módulo de IA         |
| `KAFKA_GROUP_ID`                  | Consumer group do vip-ia                                       |
| `AWS_S3_REGION` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Acesso ao S3                     |
| `AWS_S3_RECORDINGS_BUCKET`        | Bucket das gravações (o mesmo em que o vip-worker faz upload)  |
| `DEEPGRAM_API_KEY`                | Chave da Deepgram                                              |
| `OPENROUTER_API_KEY`              | Chave do OpenRouter (fase 2)                                   |
| `AUTH_JWT_SECRET`                 | **Mesmo segredo** do manager — valida o JWT (HMAC512)          |
| `DEPLOY_HOST`                     | Host usado pelo `deploy-production.sh`                         |

## Arquitetura

### Camadas

Mesmo padrão dos outros backends: `*Controller.kt` → `*Service.kt` → `*Repository.kt` (Spring Data JDBC) →
`*Entity.kt`, mais `dto/` e `enums/`. **Multi-tenancy por `companyId` em todas as consultas** — o `companyId`
vem sempre do JWT, nunca do corpo/parâmetro da requisição.

### Fila de tarefas no banco — o coração do serviço

O listener Kafka **não transcreve**: só persiste a tarefa e confirma o offset. Processadores agendados executam:

```
manager ──Kafka (KAFKA_MANAGER_IA_CDR_TOPIC)──> IaCdrListener ──> transcription (PENDING), 1 por gravação em recordNames
TranscriptionProcessor (@Scheduled + virtual threads, concorrência limitada)
    ├─ RecordingStorage (S3)      → baixa o .mp3
    ├─ TranscriptionProvider       → Deepgram (nova-3, pt-BR, diarize, utterances, punctuate, smart_format)
    ├─ grava transcript + transcription_utterance → DONE
    ├─ grava usage_record (segundos de áudio)
    └─ (fase 2) cria agent_run PENDING para cada agente automático da empresa
AgentRunProcessor (fase 2) → ChatClient/OpenRouter → agent_run DONE + usage_record (tokens/custo)
```

- **Idempotência:** chave única `(company_id, cdr_unique_id, record_name)`; mensagem repetida é ignorada.
- **Estados:** `PENDING → PROCESSING → DONE`; `RETRY` (backoff exponencial via `attempts`/`next_attempt_at`) →
  `FAILED` após o limite. CDR sem gravação é ignorada (só log).
- **Gravação ainda não existe no S3** é esperado: o vip-worker publica a CDR **antes** de converter e subir o
  `.mp3` (`CdrService.sendCdr` no worker) → `RETRY`.
- No boot, tarefas presas em `PROCESSING` voltam para `RETRY`.
- Integrações externas (Deepgram, S3, ChatModel) ficam **atrás de interfaces próprias** para serem trocadas por
  fakes nos testes. Nunca chame a Deepgram ou o OpenRouter reais em testes.

### Segurança

- Sessão STATELESS; um filtro valida o JWT do manager (`HMAC512(AUTH_JWT_SECRET)`, issuer `"VIP API"`) e extrai
  `companyId`, `tenantId`, `roles` (e `managingCompany`, quando presente). **Não há login no vip-ia.**
- Roles vêm do `RoleEnum` do manager (cumulativas): agentes da empresa exigem `ROLE_COMPANY_ADMIN`; agentes da
  plataforma (`owner_company_id = NULL`) exigem `ROLE_SAAS_ADMIN`; `/internal/usage` exige `ROLE_SAAS_FINANCE`.
- Liberados sem token: apenas `/actuator/health` e `/actuator/prometheus` (proteção de rede, como nos outros apps).

### Contrato Kafka

A mensagem consumida é produzida pelo **vip-pabx-manager**. O contrato fica num golden file em
`src/test/resources/kafka-contracts/` — ao mexer na classe da mensagem, atualize o golden file e lembre que o
manager tem o contrato espelhado.

## Banco / Migrações

- Spring Data **JDBC**, MySQL 8 próprio (não compartilhado com o manager).
- Sem Flyway: `src/main/resources/db/migration/*.sql` é o **registro versionado** do schema
  (`create_<tabela>.sql` / `update_<descricao>.sql`); aplicar em produção é manual.
- **Nunca altere um `.sql` já existente** — crie um novo arquivo para qualquer mudança.
- `src/test/resources/schema.sql` é mantido **à mão**: reflita nele toda migration nova, ou os testes quebram.
- Ao criar uma migration, registre um log com data e os scripts a rodar em produção.

## Testes

- H2 em memória (modo MySQL), schema via `src/test/resources/schema.sql`.
- Kafka auto-configuration excluída nos testes.
- Fakes para `RecordingStorage`, `TranscriptionProvider` e `ChatModel`.
- Testes de segurança obrigatórios para endpoints novos: sem token, token inválido, isolamento por `companyId` e,
  na fase 2, visibilidade de agentes (`PRIVATE` x `PUBLIC`).

## Convenções

- Imutabilidade: `val` e `data class` por padrão; injeção **por construtor** apenas (nunca `@Autowired` em campo).
- I/O concorrente com virtual threads; limite explícito de concorrência para Deepgram e OpenRouter.
- Custos externos (segundos de áudio, tokens, custo) **sempre** gravados em `usage_record` — o relatório
  financeiro do manager lê direto do vip-ia.

## Definição de Trabalho Feito (Definition of Done)

* **Verificação técnica (`verification-before-completion` & `test-driven-development`)**: Uma alteração só é considerada
  feita quando todos os testes passam com sucesso. Utilize `test-driven-development` durante a implementação e acione a
  skill `verification-before-completion` antes de comitar ou declarar conclusão, garantindo a exibição das evidências de
  execução dos testes. Nunca pule, desabilite ou enfraqueça testes; em caso de falhas imprevistas, aplique a skill
  `systematic-debugging`.
* **Revisão automatizada (`requesting-code-review`)**: Nenhuma branch deve ser fundida (merge) na branch `dev` sem
  passar por uma revisão de código automatizada. Invoque a skill `requesting-code-review` para solicitar a validação por
  um agente independente (diferente daquele que escreveu o código), utilizando um modelo mais capaz (ex: Opus).
* **Processo de revisão (`receiving-code-review`)**: Ao receber o feedback da revisão, utilize a skill
  `receiving-code-review` para analisar o retorno e implementar as correções com rigor técnico (evitando concordância
  performática). Se a verificação falhar, o agente deve corrigir a falha e repetir o ciclo (testes → revisão) até passar
  (limite máximo de 5 rounds). A revisão deve ser comitada com uma mensagem clara.
* **Convenções e Planos (`executing-plans` & `writing-plans`)**: A execução do trabalho deve respeitar os planos
  estabelecidos. Não edite `to-do.md` (ou qualquer outro arquivo de plano criado pelo usuário ou por outro agente)
  diretamente no worktree fora do fluxo formal gerenciado por `executing-plans`.
* **Subagentes (`subagent-driven-development` & `using-git-worktrees`)**: Tarefas delegadas a subagentes (utilizando
  modelos mais simples como Haiku) devem ser executadas via `subagent-driven-development` (ou
  `dispatching-parallel-agents` para múltiplos fluxos independentes) e trabalhar estritamente em worktrees isoladas
  criadas via `using-git-worktrees`, para não interferir no desenvolvimento em andamento.
* **Merge final (`finishing-a-development-branch`)**: Com o trabalho feito (testes validados com evidências e revisão
  aprovada), execute a skill `finishing-a-development-branch` para conduzir a integração na branch `dev` e realizar a
  limpeza correta do ambiente e das worktrees.
