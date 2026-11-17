# transcription/api/dto

## Overview
DTOs da API HTTP de transcrições (`/transcriptions`): filtro de busca e respostas de resumo e detalhe.

## Interface
- `TranscriptionFilter(companyId, from: Instant?, to: Instant?, src?, destination?, peer?, status: TranscriptionStatus?, page: Int, size: Int)`
- `TranscriptionSummary(id, cdrUniqueId, recordName, status, callStartTime, src, destination, peer, disposition, callDuration, audioSeconds?, attempts, lastError?)`, com construtor a partir de `TranscriptionEntity`.
- `UtteranceResponse(speaker, startSeconds, endSeconds, content, confidence?)`, com construtor a partir de `TranscriptionUtteranceEntity`.
- `TranscriptionDetail(summary: TranscriptionSummary, transcript: String?, language: String?, model: String?, utterances: List<UtteranceResponse>)`

## Invariants
- `TranscriptionSummary(entity)` exige `entity.id` não nulo (`requireNotNull`).
- `TranscriptionFilter.companyId` vem sempre do token (`UserPrincipal.companyId`), nunca de parâmetro da requisição.
- Os DTOs não expõem `userfield` nem `deepgramRequestId`.

## Side Effects
- None

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.transcription.api.*'` (na raiz `vip-ia/`)
