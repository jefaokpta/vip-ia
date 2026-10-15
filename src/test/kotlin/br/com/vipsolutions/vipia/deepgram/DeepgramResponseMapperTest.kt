package br.com.vipsolutions.vipia.deepgram

import br.com.vipsolutions.vipia.transcription.provider.Utterance
import com.deepgram.core.ObjectMappers
import com.deepgram.types.ListenV1Response
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DeepgramResponseMapperTest {

    private fun load(name: String): ListenV1Response =
        ObjectMappers.JSON_MAPPER.readValue(
            javaClass.getResourceAsStream("/deepgram/$name")!!.readAllBytes(),
            ListenV1Response::class.java,
        )

    @Test
    fun `mapeia texto, falas por locutor, duracao e request id`() {
        val result = DeepgramResponseMapper.toResult(load("response-with-speech.json"), "pt-BR")

        assertEquals("Olá, bom dia. Bom dia, em que posso ajudar?", result.transcript)
        assertEquals(42.5, result.audioSeconds)
        assertEquals("req-123", result.requestId)
        assertEquals("nova-3", result.model)
        assertEquals("pt-BR", result.language)
        assertEquals(
            listOf(
                Utterance(speaker = 0, startSeconds = 0.5, endSeconds = 1.75, content = "Olá, bom dia.", confidence = 0.97f.toDouble()),
                Utterance(speaker = 1, startSeconds = 2.0, endSeconds = 4.0, content = "Bom dia, em que posso ajudar?", confidence = 0.95f.toDouble()),
            ),
            result.utterances,
        )
    }

    @Test
    fun `resposta sem fala gera resultado vazio`() {
        val result = DeepgramResponseMapper.toResult(load("response-silence.json"), "pt-BR")

        assertEquals("", result.transcript)
        assertTrue(result.utterances.isEmpty())
        assertEquals(7.0, result.audioSeconds)
    }
}
