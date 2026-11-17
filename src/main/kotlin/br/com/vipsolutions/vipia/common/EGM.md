# common

## Overview
Tipos utilitários compartilhados entre as APIs HTTP do serviço.

## Interface
- `PageResponse<T>(content: List<T>, page: Int, size: Int, totalElements: Long)`: envelope de paginação retornado por `GET /transcriptions` (via `TranscriptionQueryRepository.search`).

## Invariants
- `page` é zero-based e `totalElements` é a contagem total sem paginação (preenchido por `TranscriptionQueryRepository.search`).

## Side Effects
- None

## Verification
- Run: `./tests.sh` (na raiz `vip-ia/`)
