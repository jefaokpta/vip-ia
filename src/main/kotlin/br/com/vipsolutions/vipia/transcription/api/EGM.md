# transcription/api

## Overview
API REST de consulta e reprocessamento de transcrições, usada pelo frontend. Toda operação é escopada pelo `companyId` do JWT autenticado.

## Interface
- `TranscriptionController` (`@RestController`, `@RequestMapping("/transcriptions")`)
  - `GET /transcriptions` → `PageResponse<TranscriptionSummary>`; query params opcionais `from`, `to`, `src`, `destination`, `peer`, `status`, `page` (padrão `0`), `size` (padrão `20`).
  - `GET /transcriptions/{id}` → `TranscriptionDetail`
  - `GET /transcriptions/by-cdr/{uniqueId}` → `List<TranscriptionDetail>` (ordenado por `recordName`)
  - `POST /transcriptions/{id}/retry` → `204 No Content`
- `TranscriptionApiService`: `search(filter)`, `detail(id, companyId)`, `byCdr(uniqueId, companyId)`, `retry(id, companyId)`.
- `TranscriptionQueryRepository.search(filter: TranscriptionFilter): PageResponse<TranscriptionSummary>`: SQL dinâmico com `NamedParameterJdbcTemplate`.
- Subdiretório: [`dto`](./dto/EGM.md) - DTOs de filtro e resposta.

## Invariants
- `companyId` vem de `@AuthenticationPrincipal UserPrincipal`; transcrição de outra empresa é tratada como inexistente (`404`).
- `page` é forçado a `>= 0` e `size` a `1..100` no controller.
- Filtro de período: `call_start_time >= from` e `call_start_time < to` (intervalo semiaberto); demais filtros são igualdade exata. Ordenação `call_start_time DESC, id DESC`.
- `retry` só aceita transcrições `FAILED`; outro status responde `409 CONFLICT`. O reset (`resetFailedForRetry`) volta a tarefa para `PENDING` com `attempts = 0` e `last_error = NULL`.
- Todos os parâmetros entram no SQL como bind parameters nomeados; apenas os nomes de colunas fixos são concatenados no `WHERE`.

## Side Effects
- Leitura das tabelas `transcription` e `transcription_utterance`.
- `POST /{id}/retry` faz `UPDATE` em `transcription` (devolve a tarefa à fila de processamento).

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.transcription.api.*'` (na raiz `vip-ia/`)
