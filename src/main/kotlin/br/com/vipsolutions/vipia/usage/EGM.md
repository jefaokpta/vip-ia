# usage

## Overview
Registro de consumo de provedores externos (base do faturamento), persistido na tabela `usage_record`, e o relatório interno em [`api`](./api/EGM.md).

## Interface
- `UsageService.recordTranscription(companyId: String, transcriptionId: Long, audioSeconds: Double, model: String, now: Instant)`: grava um registro `TRANSCRIPTION`/`DEEPGRAM`.
- `UsageRecordEntity` (`@Table("usage_record")`): `id`, `companyId`, `usageType`, `referenceId`, `provider`, `model?`, `audioSeconds?`, `inputTokens?`, `outputTokens?`, `cost?`, `createdAt`.
- `UsageRecordRepository` (`CrudRepository<UsageRecordEntity, Long>`): `findByReferenceIdAndUsageType(referenceId, usageType)`.
- `UsageProvider { DEEPGRAM, OPENROUTER }`
- `UsageType { TRANSCRIPTION, AGENT_RUN }`
- Subdiretório: [`api`](./api/EGM.md) - `GET /internal/usage`.

## Invariants
- Em `TRANSCRIPTION`, `referenceId` é o `id` da tabela `transcription` e `cost` fica `NULL` (a Deepgram não devolve preço; o financeiro aplica o preço por minuto sobre `audioSeconds`).
- `recordTranscription` é chamado dentro da transação de `TranscriptionCompletionService.complete`: o consumo só existe se a transcrição foi marcada `DONE`.
- `OPENROUTER` e `AGENT_RUN` existem no enum mas não têm gravação no código atual.

## Side Effects
- `INSERT` na tabela `usage_record`.

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.usage.*'` (na raiz `vip-ia/`)
