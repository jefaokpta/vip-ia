package br.com.vipsolutions.vipia.transcription.provider

/** Serviço de speech-to-text. Em produção é a Deepgram; nos testes, um fake. */
interface TranscriptionProvider {

    /** @throws TranscriptionFailedException com retryable indicando se vale tentar de novo. */
    fun transcribe(audio: ByteArray): TranscriptionResult
}

data class TranscriptionResult(
    val transcript: String,
    val utterances: List<Utterance>,
    val audioSeconds: Double,
    val language: String,
    val model: String,
    val requestId: String,
)

data class Utterance(
    val speaker: Int,
    val startSeconds: Double,
    val endSeconds: Double,
    val content: String,
    val confidence: Double?,
)

class TranscriptionFailedException(
    message: String,
    val retryable: Boolean,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
