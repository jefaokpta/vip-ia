# transcription

## Overview
Núcleo do serviço: fila persistente de tarefas de transcrição (uma por gravação de uma CDR) na tabela `transcription`. As tarefas são criadas pelo consumidor Kafka, processadas periodicamente pelo `TranscriptionScheduler`/`TranscriptionProcessor` (download do S3 → speech-to-text → gravação do resultado e do consumo) e expostas pela API em [`api`](./api/EGM.md).

## Interface
- `TranscriptionTaskService.enqueue(message: IaCdrMessage): Int`: cria uma tarefa `PENDING` por `recordName` (aparado, não vazio, distinto); retorna quantas foram criadas.
- `TranscriptionProcessor`
  - `processDueTasks(): Int`: devolve à fila tarefas `PROCESSING` presas há mais de `stuckTimeout`, busca até `batchSize` tarefas vencidas e processa cada uma numa virtual thread, limitado por `Semaphore(maxConcurrency)`.
  - `processOne(task: TranscriptionEntity)`
- `TranscriptionScheduler.run()`: `@Scheduled(fixedDelayString = "${vip-ia.transcription.poll-interval}")`, condicionado a `vip-ia.scheduling.enabled`.
- `TranscriptionCompletionService.complete(task, result)`: `@Transactional`.
- `StuckTranscriptionRecovery.recover(): Int`: `@EventListener(ApplicationReadyEvent)`.
- `RetryPolicy`: `nextAttemptAt(attempts: Int, now: Instant): Instant`, `shouldGiveUp(attempts: Int): Boolean`.
- `TranscriptionRepository` (`CrudRepository<TranscriptionEntity, Long>`): `existsByCompanyIdAndCdrUniqueIdAndRecordName`, `findByIdAndCompanyId`, `findByCompanyIdAndCdrUniqueIdOrderByRecordName`, `findDue`, `claim`, `markRetry`, `markFailed`, `markDone`, `resetStuck`, `resetStale`, `resetFailedForRetry`.
- `TranscriptionUtteranceRepository`: `findByTranscriptionIdOrderByStartSeconds`, `deleteByTranscriptionId`.
- Entidades: `TranscriptionEntity` (`@Table("transcription")`), `TranscriptionUtteranceEntity` (`@Table("transcription_utterance")`).
- `TranscriptionStatus { PENDING, PROCESSING, RETRY, DONE, FAILED }`
- `TranscriptionProperties` (`@ConfigurationProperties(prefix = "vip-ia.transcription")`): `pollInterval`, `batchSize`, `maxConcurrency`, `maxAttempts`, `retryBaseDelay`, `language`, `stuckTimeout` (padrão 10 min).
- Subdiretórios:
  - [`api`](./api/EGM.md) - API REST `/transcriptions`.
  - [`provider`](./provider/EGM.md) - porta `TranscriptionProvider` e tipos de resultado.

## Invariants
- Idempotência: a chave única `(company_id, cdr_unique_id, record_name)` impede duplicar tarefas em reentregas do Kafka; `enqueue` checa `exists` e trata `DuplicateKeyException` da corrida como "já enfileirada".
- Mudanças de estado são feitas só por `UPDATE`s explícitos do `TranscriptionRepository`, nunca por `save()` da entidade inteira (evita sobrescrever campos em corrida). `save()` é usado apenas para inserir a tarefa nova.
- `claim` é a reivindicação atômica: só quem muda `PENDING`/`RETRY` → `PROCESSING` (retorno `1`) processa a tarefa.
- Máquina de estados: `PENDING`/`RETRY` → `PROCESSING` → `DONE` | `RETRY` | `FAILED`; `FAILED` → `PENDING` somente via retry manual da API.
- Falha: `RecordingNotFoundException` e erros inesperados são retentáveis; `TranscriptionFailedException` respeita `retryable`. Vira `FAILED` se não retentável ou se `attempts >= maxAttempts`; senão `RETRY` com `next_attempt_at = now + retryBaseDelay * 2^(attempts-1)` (expoente limitado a `0..20`). `last_error` é truncado em 1000 caracteres.
- `resetStale` (varredura periódica) conta a volta à fila como uma tentativa (`attempts + 1`); `resetStuck` (no boot) não conta.
- `StuckTranscriptionRecovery` assume **uma única instância** do vip-ia por banco: com várias instâncias, roubaria tarefas em andamento.
- `stuckTimeout` deve ser maior que `deepgram.timeout-seconds` + tempo de download do S3.
- `complete` grava `markDone`, substitui as utterances (`deleteByTranscriptionId` + `saveAll`) e registra o consumo (`UsageService.recordTranscription`) numa única transação.
- Horários vêm do bean `Clock` injetado.

## Side Effects
- Leitura/escrita nas tabelas `transcription`, `transcription_utterance` e (via `UsageService`) `usage_record`.
- Download de áudio do S3 (`RecordingStorage`) e chamada paga à Deepgram (`TranscriptionProvider`).
- Cria virtual threads por varredura (`Executors.newVirtualThreadPerTaskExecutor`).
- Logs por tarefa (sucesso, reagendamento, falha definitiva, tarefas presas).

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.transcription.*'` (na raiz `vip-ia/`)
