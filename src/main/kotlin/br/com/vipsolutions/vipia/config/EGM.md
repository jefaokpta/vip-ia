# config

## Overview
Configuração transversal do Spring: relógio único da aplicação e ativação condicional do agendamento.

## Interface
- `ClockConfig.clock(): Clock`: bean `Clock.systemUTC()` injetado em todo código que calcula horários (backoff, `created_at`, `updated_at`).
- `SchedulingConfig`: `@EnableScheduling` condicionado a `vip-ia.scheduling.enabled=true` (padrão quando ausente).

## Invariants
- Código que precisa do horário atual deve usar o bean `Clock` injetado (nunca `Instant.now()` direto), para os testes poderem fixar o relógio.
- Todos os horários são UTC (`Clock.systemUTC()`).
- Com `vip-ia.scheduling.enabled=false` (usado nos testes) nenhum `@Scheduled` roda; o mesmo condicional existe em `TranscriptionScheduler`.

## Side Effects
- `SchedulingConfig` liga o executor de tarefas agendadas do Spring.

## Verification
- Run: `./tests.sh` (na raiz `vip-ia/`)
