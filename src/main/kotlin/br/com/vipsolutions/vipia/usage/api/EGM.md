# usage/api

## Overview
Endpoint interno de relatório de consumo por empresa, consumido pelo relatório financeiro do manager.

## Interface
- `UsageController` (`@RestController`, `@RequestMapping("/internal/usage")`)
  - `GET /internal/usage?companyId=&from=&to=` → `UsageReport`, protegido por `@PreAuthorize("hasAuthority('ROLE_SAAS_FINANCE')")`.
- `UsageReportRepository.summarize(companyId: String, from: Instant, to: Instant): List<UsageSummary>`
- Subdiretório: [`dto`](./dto/EGM.md) - `UsageSummary` e `UsageReport`.

## Invariants
- Exige a authority `ROLE_SAAS_FINANCE`; as roles do manager são cumulativas (`SAAS_ADMIN` também tem `SAAS_FINANCE`).
- Diferente de `/transcriptions`, o `companyId` vem do parâmetro da requisição (não do token): o acesso é controlado pela role.
- Período semiaberto: `created_at >= from AND created_at < to`.
- Agregação `GROUP BY provider, usage_type, model`, ordenada pelas mesmas colunas; somas de `audio_seconds`/`input_tokens`/`output_tokens` usam `COALESCE(..., 0)` e `cost` é `SUM(cost)` (pode ser `null`).
- Sem CORS configurado: não é chamado pelo navegador.

## Side Effects
- Leitura da tabela `usage_record`.

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.usage.*'` (na raiz `vip-ia/`)
