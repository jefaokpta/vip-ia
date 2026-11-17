# usage/api/dto

## Overview
DTOs do relatório interno de consumo de provedores externos.

## Interface
- `UsageSummary(provider: UsageProvider, usageType: UsageType, model: String?, records: Long, audioSeconds: Double, inputTokens: Long, outputTokens: Long, cost: BigDecimal?)`
- `UsageReport(companyId: String, from: Instant, to: Instant, items: List<UsageSummary>)`

## Invariants
- `cost` é `null` quando o provedor não informa custo (Deepgram); o financeiro aplica o preço por minuto sobre `audioSeconds`.

## Side Effects
- None

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.usage.*'` (na raiz `vip-ia/`)
