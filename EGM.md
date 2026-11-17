# vip-ia Blueprint

## System Overview
`vip-ia` é o serviço de IA da plataforma PABX (Kotlin 2.3 / Spring Boot 4.1 / JDK 25 com virtual threads, porta `8083`). Hoje implementa a transcrição de chamadas: consome as CDRs que o vip-pabx-manager republica num tópico Kafka dedicado (só empresas com o módulo de IA), cria uma tarefa por gravação no MySQL próprio, baixa o áudio do S3 (subido pelo vip-worker), transcreve com a Deepgram e registra o resultado e o consumo. Visão e escopo completos em `MANIFESTO.md`; regras de trabalho em `AGENTS.md`.

```text
Kafka (KAFKA_MANAGER_IA_CDR_TOPIC)
  └─ IaCdrListener ─► TranscriptionTaskService.enqueue ─► MySQL transcription (PENDING)
TranscriptionScheduler (poll-interval) ─► TranscriptionProcessor.processDueTasks
  ├─ claim (PROCESSING)
  ├─ RecordingStorage.download ─► S3 recordings/{companyId}/{recordName}.mp3
  ├─ TranscriptionProvider.transcribe ─► Deepgram (nova-3)
  └─ TranscriptionCompletionService.complete (1 transação)
       ─► transcription (DONE) + transcription_utterance + usage_record
HTTP (JWT do manager)
  ├─ /transcriptions/**   (frontend, escopado pelo companyId do token)
  └─ /internal/usage      (relatório financeiro, ROLE_SAAS_FINANCE)
```

## Subdirectories & System Boundaries
- src: [`./src/main/EGM.md`](./src/main/EGM.md) - fontes de produção.
  - Pacote principal: [`./src/main/kotlin/br/com/vipsolutions/vipia/EGM.md`](./src/main/kotlin/br/com/vipsolutions/vipia/EGM.md) - `VipIaApplication` e índice dos módulos.
  - [`auth`](./src/main/kotlin/br/com/vipsolutions/vipia/auth/EGM.md) - valida o JWT HMAC512 emitido pelo manager; sessão stateless; `UserPrincipal.companyId` isola os dados.
  - [`kafka`](./src/main/kotlin/br/com/vipsolutions/vipia/kafka/EGM.md) - consumidor SASL/SCRAM das CDRs de IA; descarta mensagens inválidas; retry infinito com backoff quando o banco cai.
  - [`transcription`](./src/main/kotlin/br/com/vipsolutions/vipia/transcription/EGM.md) - fila persistente `PENDING → PROCESSING → DONE | RETRY | FAILED`, processador concorrente, `RetryPolicy` e API `/transcriptions`.
  - [`deepgram`](./src/main/kotlin/br/com/vipsolutions/vipia/deepgram/EGM.md) - implementação de `TranscriptionProvider` sobre o SDK da Deepgram.
  - [`recording`](./src/main/kotlin/br/com/vipsolutions/vipia/recording/EGM.md) - leitura das gravações no S3.
  - [`usage`](./src/main/kotlin/br/com/vipsolutions/vipia/usage/EGM.md) - registro de consumo de provedores e API `/internal/usage`.
  - [`config`](./src/main/kotlin/br/com/vipsolutions/vipia/config/EGM.md) / [`common`](./src/main/kotlin/br/com/vipsolutions/vipia/common/EGM.md) - `Clock` UTC, agendamento condicional, `PageResponse`.
  - [`resources`](./src/main/resources/db/migration/EGM.md) - `application.yml` e [`db/migration`](./src/main/resources/db/migration/EGM.md) (DDL).

## Global Invariants
- Isolamento por empresa: toda leitura de `/transcriptions` usa o `companyId` do JWT; recurso de outra empresa responde `404`.
- Idempotência de ingestão: chave única `(company_id, cdr_unique_id, record_name)` na tabela `transcription`; reentregas do Kafka não duplicam trabalho nem custo.
- Estado das tarefas só muda por `UPDATE`s explícitos e condicionais do `TranscriptionRepository` (reivindicação atômica via `claim`), nunca por `save()` da entidade inteira.
- Retry é feito pela aplicação (`RetryPolicy`: base `retry-base-delay` × 2^(tentativas−1), até `max-attempts`); o SDK da Deepgram roda com `maxRetries(0)`.
- Resultado, utterances e consumo são gravados numa única transação (`TranscriptionCompletionService.complete`).
- Uma única instância do vip-ia por banco (`StuckTranscriptionRecovery` devolve todo `PROCESSING` para `RETRY` no boot).
- Horários sempre pelo bean `Clock` (UTC).
- Schema sem Flyway: migrations em `src/main/resources/db/migration` nunca são editadas, só acrescentadas, e devem ser espelhadas em `src/test/resources/schema.sql`.
- O contrato `IaCdrMessage` é espelhado no manager (golden file `src/test/resources/kafka-contracts/ia-cdr-message.json`).

## Global Side Effects
- Kafka: consumo do tópico `KAFKA_MANAGER_IA_CDR_TOPIC` (grupo `KAFKA_GROUP_ID`).
- MySQL 8 próprio: tabelas `transcription`, `transcription_utterance`, `usage_record`.
- AWS S3: leitura do bucket `AWS_S3_RECORDINGS_BUCKET`.
- Deepgram: chamadas HTTP pagas de speech-to-text (modelo `nova-3`, idioma `pt-BR`).
- HTTP: `/transcriptions/**`, `/internal/usage`, `/actuator/health`, `/actuator/prometheus`.

## System Verification
- Run: `./tests.sh` (carrega o `.env` e executa `./gradlew test`; o `AGENTS.md` pede para não rodar `./gradlew test` puro, exceto para teste único)
- Teste único/pacote: `./tests.sh --tests 'br.com.vipsolutions.vipia.<pacote>.*'`
