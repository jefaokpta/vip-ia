# vipia

## Overview
Pacote raiz do serviço vip-ia (`br.com.vipsolutions.vipia`): ponto de entrada Spring Boot e agrupamento dos módulos de autenticação, ingestão Kafka, armazenamento de gravações, transcrição (Deepgram) e consumo.

Fluxo principal: Kafka (`IaCdrListener`) → `TranscriptionTaskService.enqueue` (tabela `transcription`, `PENDING`) → `TranscriptionScheduler`/`TranscriptionProcessor` → `RecordingStorage.download` (S3) → `TranscriptionProvider.transcribe` (Deepgram) → `TranscriptionCompletionService.complete` (`transcription`, `transcription_utterance`, `usage_record`) → leitura via `/transcriptions` e `/internal/usage`.

## Interface
- `VipIaApplication` (`@SpringBootApplication`, `@ConfigurationPropertiesScan`) e `main(args)`.
- Subdiretórios:
  - [`auth`](./auth/EGM.md) - validação do JWT do manager, `SecurityConfig`, `UserPrincipal`.
  - [`common`](./common/EGM.md) - `PageResponse<T>`.
  - [`config`](./config/EGM.md) - bean `Clock` (UTC) e `SchedulingConfig` condicional.
  - [`deepgram`](./deepgram/EGM.md) - `DeepgramTranscriptionProvider` (implementa `TranscriptionProvider`).
  - [`kafka`](./kafka/EGM.md) - consumidor `IaCdrListener`, parser e configuração SASL/SCRAM.
  - [`recording`](./recording/EGM.md) - `RecordingStorage` / `S3RecordingStorage`.
  - [`transcription`](./transcription/EGM.md) - fila de tarefas, processador, retry e API `/transcriptions`.
  - [`usage`](./usage/EGM.md) - registro de consumo e API `/internal/usage`.

## Invariants
- `@ConfigurationPropertiesScan` registra `DeepgramProperties` e `TranscriptionProperties` sem `@EnableConfigurationProperties` explícito.
- Dependências de infraestrutura entram por portas (`RecordingStorage`, `TranscriptionProvider`) para os testes trocarem por fakes.

## Side Effects
- Sobe o contexto Spring: servidor HTTP, consumidor Kafka, agendamento e conexões com MySQL, S3 e Deepgram (detalhes nos subpacotes).

## Verification
- Run: `./tests.sh` (na raiz `vip-ia/`)
