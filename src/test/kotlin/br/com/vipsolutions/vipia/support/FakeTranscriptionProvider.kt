package br.com.vipsolutions.vipia.support

import br.com.vipsolutions.vipia.transcription.provider.TranscriptionProvider
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionResult
import br.com.vipsolutions.vipia.transcription.provider.Utterance

class FakeTranscriptionProvider : TranscriptionProvider {

    var result: TranscriptionResult = sampleResult()
    var error: Exception? = null
    var calls = 0

    override fun transcribe(audio: ByteArray): TranscriptionResult {
        calls++
        error?.let { throw it }
        return result
    }

    companion object {
        fun sampleResult() = TranscriptionResult(
            transcript = "Olá, bom dia. Bom dia, em que posso ajudar?",
            utterances = listOf(
                Utterance(speaker = 0, startSeconds = 0.5, endSeconds = 1.75, content = "Olá, bom dia.", confidence = 0.97),
                Utterance(speaker = 1, startSeconds = 2.0, endSeconds = 4.0, content = "Bom dia, em que posso ajudar?", confidence = 0.95),
            ),
            audioSeconds = 42.5,
            language = "pt-BR",
            model = "nova-3",
            requestId = "req-123",
        )
    }
}
