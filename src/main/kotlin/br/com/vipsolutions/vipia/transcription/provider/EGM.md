# transcription/provider

## Overview
Porta (interface) de speech-to-text usada pelo pipeline de transcrição, independente de fornecedor. Em produção é implementada por `DeepgramTranscriptionProvider` ([`../../deepgram`](../../deepgram/EGM.md)); nos testes, por um fake.

## Interface
- `TranscriptionProvider` (interface)
  - `transcribe(audio: ByteArray): TranscriptionResult`
- `TranscriptionResult(transcript: String, utterances: List<Utterance>, audioSeconds: Double, language: String, model: String, requestId: String)`
- `Utterance(speaker: Int, startSeconds: Double, endSeconds: Double, content: String, confidence: Double?)`
- `TranscriptionFailedException(message: String, retryable: Boolean, cause: Throwable? = null)`

## Invariants
- Falhas de transcrição devem ser sinalizadas com `TranscriptionFailedException`; o campo `retryable` decide entre `RETRY` (com backoff) e `FAILED` imediato no `TranscriptionProcessor`.

## Side Effects
- None (apenas contrato; os efeitos ficam na implementação).

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.transcription.*'` (na raiz `vip-ia/`)
