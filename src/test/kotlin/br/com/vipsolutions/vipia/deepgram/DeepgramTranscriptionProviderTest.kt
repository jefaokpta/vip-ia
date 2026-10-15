package br.com.vipsolutions.vipia.deepgram

import br.com.vipsolutions.vipia.transcription.TranscriptionProperties
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionFailedException
import com.deepgram.DeepgramClient
import com.deepgram.core.DeepgramApiException
import com.deepgram.core.DeepgramHttpException
import com.deepgram.core.ObjectMappers
import com.deepgram.errors.BadRequestError
import com.deepgram.resources.listen.v1.media.requests.MediaTranscribeRequestOctetStream
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeRequestModel
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeResponse
import com.deepgram.types.ListenV1Response
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class DeepgramTranscriptionProviderTest {

    private val client = mockk<DeepgramClient>()
    private val properties = TranscriptionProperties(
        pollInterval = Duration.ofSeconds(10),
        batchSize = 20,
        maxConcurrency = 4,
        maxAttempts = 6,
        retryBaseDelay = Duration.ofMinutes(1),
        language = "pt-BR",
    )
    private val provider = DeepgramTranscriptionProvider(client, properties)
    private val audio = byteArrayOf(9, 9, 9)

    private fun speechResponse(): MediaTranscribeResponse = MediaTranscribeResponse.of(
        ObjectMappers.JSON_MAPPER.readValue(
            javaClass.getResourceAsStream("/deepgram/response-with-speech.json")!!.readAllBytes(),
            ListenV1Response::class.java,
        )
    )

    private fun givenDeepgramThrows(error: Exception) {
        every { client.listen().v1().media().transcribeFile(any<MediaTranscribeRequestOctetStream>()) } throws error
    }

    @Test
    fun `envia o audio com nova-3, pt-BR, diarizacao e utterances`() {
        val request = slot<MediaTranscribeRequestOctetStream>()
        every { client.listen().v1().media().transcribeFile(capture(request)) } returns speechResponse()

        val result = provider.transcribe(audio)

        assertEquals("req-123", result.requestId)
        assertArrayEquals(audio, request.captured.body)
        assertEquals(MediaTranscribeRequestModel.NOVA3, request.captured.model.get())
        assertEquals("pt-BR", request.captured.language.get())
        assertTrue(request.captured.diarize.get())
        assertTrue(request.captured.utterances.get())
        assertTrue(request.captured.punctuate.get())
        assertTrue(request.captured.smartFormat.get())
    }

    @Test
    fun `erro 5xx e retentavel`() {
        givenDeepgramThrows(DeepgramHttpException("indisponivel", 503, "Service Unavailable"))

        assertTrue(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }

    @Test
    fun `erro 429 e retentavel`() {
        givenDeepgramThrows(DeepgramHttpException("rate limit", 429, "Too Many Requests"))

        assertTrue(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }

    @Test
    fun `erro 400 nao e retentavel`() {
        givenDeepgramThrows(BadRequestError("audio corrompido"))

        assertFalse(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }

    @Test
    fun `erros de conta 401, 402 e 403 sao retentaveis`() {
        // chave expirada ou saldo esgotado nao pode marcar todas as tarefas como FAILED de uma vez
        listOf(401, 402, 403).forEach { status ->
            givenDeepgramThrows(DeepgramHttpException("conta", status, "erro de conta"))

            assertTrue(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable, "HTTP $status")
        }
    }

    @Test
    fun `erro 413 nao e retentavel`() {
        givenDeepgramThrows(DeepgramHttpException("grande demais", 413, "Payload Too Large"))

        assertFalse(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }

    @Test
    fun `falha de rede e retentavel`() {
        givenDeepgramThrows(DeepgramApiException("timeout"))

        assertTrue(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }
}
