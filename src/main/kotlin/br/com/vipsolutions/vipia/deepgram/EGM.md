# deepgram

## Overview
Adaptador de speech-to-text para a Deepgram (SDK `com.deepgram:deepgram-java-sdk`). Implementa a porta `TranscriptionProvider` de [`../transcription/provider`](../transcription/provider/EGM.md) e converte a resposta do SDK para `TranscriptionResult`.

## Interface
- `DeepgramTranscriptionProvider` (`@Component`, implementa `TranscriptionProvider`)
  - `transcribe(audio: ByteArray): TranscriptionResult`: envia o áudio como octet-stream com modelo `NOVA3`, `language = TranscriptionProperties.language`, `diarizeModel(V1)`, `utterances`, `punctuate` e `smartFormat` ligados.
- `DeepgramResponseMapper` (`object`)
  - `const val MODEL = "nova-3"`
  - `toResult(response: ListenV1Response, language: String): TranscriptionResult`
- `DeepgramConfig.deepgramClient(): DeepgramClient`: cliente com `apiKey`, `timeout` e `maxRetries(0)`.
- `DeepgramProperties(apiKey: String, timeoutSeconds: Int)`: `@ConfigurationProperties(prefix = "deepgram")`.

## Invariants
- O SDK não faz retry (`maxRetries(0)`): retry com backoff é responsabilidade do `TranscriptionProcessor`/`RetryPolicy`.
- Mapeamento de erros para `TranscriptionFailedException.retryable`:
  - `DeepgramHttpException` com status em `{401, 402, 403, 408, 429}` ou `>= 500` → `retryable = true` (problema de conta/saldo não deve gerar FAILED em massa); outros status → `false`.
  - `DeepgramApiException` (falha de comunicação) → `retryable = true`.
  - Resposta `ListenV1AcceptedResponse` (modo callback) → `retryable = false`.
- `DeepgramResponseMapper.toResult` descarta utterances com conteúdo vazio após `trim()`, usa `speaker` `0`, `start`/`end` `0` quando ausentes, e pega o transcript do primeiro canal/primeira alternativa (ou `""`).
- `model` gravado no resultado é sempre `"nova-3"`; `audioSeconds` vem de `metadata.duration` e `requestId` de `metadata.requestId`.

## Side Effects
- Chamada HTTP síncrona à API da Deepgram (`client.listen().v1().media().transcribeFile`), com timeout `deepgram.timeout-seconds` (300 s em `application.yml`). Gera custo por áudio transcrito.

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.deepgram.*'` (na raiz `vip-ia/`)
