package br.com.vipsolutions.vipia.deepgram

import br.com.vipsolutions.vipia.transcription.provider.TranscriptionResult
import br.com.vipsolutions.vipia.transcription.provider.Utterance
import com.deepgram.types.ListenV1Response

/** Converte a resposta do SDK (cheia de Optional) no nosso TranscriptionResult. */
object DeepgramResponseMapper {

    const val MODEL = "nova-3"

    fun toResult(response: ListenV1Response, language: String): TranscriptionResult {
        val utterances = response.results.utterances.orElse(emptyList())
            .map { item ->
                Utterance(
                    speaker = item.speaker.orElse(0),
                    startSeconds = item.start.orElse(0f).toDouble(),
                    endSeconds = item.end.orElse(0f).toDouble(),
                    content = item.transcript.orElse("").trim(),
                    confidence = item.confidence.map { it.toDouble() }.orElse(null),
                )
            }
            .filter { it.content.isNotEmpty() }
        val transcript = response.results.channels.firstOrNull()
            ?.alternatives?.orElse(emptyList())?.firstOrNull()
            ?.transcript?.orElse("")
            ?: ""
        return TranscriptionResult(
            transcript = transcript.trim(),
            utterances = utterances,
            audioSeconds = response.metadata.duration,
            language = language,
            model = MODEL,
            requestId = response.metadata.requestId,
        )
    }
}
