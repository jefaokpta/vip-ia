# db/migration

## Overview
Registro versionado do schema MySQL 8 do vip-ia (banco próprio, não compartilhado com o manager). Não há Flyway nas dependências e `spring.sql.init.mode` é `never` em produção: os scripts são aplicados manualmente.

## Interface
- `create_transcription.sql` → tabela `transcription`: `UNIQUE KEY uk_transcription_cdr_record (company_id, cdr_unique_id, record_name)`, `INDEX idx_transcription_company_start (company_id, call_start_time)`, `INDEX idx_transcription_due (status, next_attempt_at)`.
- `create_transcription_utterance.sql` → tabela `transcription_utterance`: `INDEX idx_utterance_transcription (transcription_id, start_seconds)`, `FOREIGN KEY fk_utterance_transcription (transcription_id) REFERENCES transcription (id) ON DELETE CASCADE`.
- `create_usage_record.sql` → tabela `usage_record`: `INDEX idx_usage_company_created (company_id, created_at)`, `INDEX idx_usage_reference (reference_id, usage_type)`.

## Invariants
- Nunca altere um `.sql` existente: toda mudança de schema é um arquivo novo (`create_<tabela>.sql` / `update_<descricao>.sql`).
- Toda migration nova precisa ser espelhada à mão em `src/test/resources/schema.sql` (H2 em modo MySQL), ou os testes quebram.
- A chave única `uk_transcription_cdr_record` sustenta a idempotência de `TranscriptionTaskService.enqueue`; `idx_transcription_due` atende `TranscriptionRepository.findDue`.
- Colunas `DATETIME(3)` guardam instantes UTC (bean `Clock.systemUTC()`); `status` armazena o nome do enum `TranscriptionStatus`, `usage_type`/`provider` os nomes de `UsageType`/`UsageProvider`.
- Apagar uma `transcription` remove suas utterances em cascata.

## Side Effects
- Nenhum em tempo de execução da aplicação; executados manualmente contra o MySQL de produção.

## Verification
- Run: `./tests.sh` (na raiz `vip-ia/`; usa o espelho `src/test/resources/schema.sql`)
