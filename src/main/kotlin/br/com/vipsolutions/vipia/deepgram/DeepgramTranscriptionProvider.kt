package br.com.vipsolutions.vipia.deepgram

import br.com.vipsolutions.vipia.transcription.TranscriptionProperties
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionFailedException
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionProvider
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionResult
import com.deepgram.DeepgramClient
import com.deepgram.core.DeepgramApiException
import com.deepgram.core.DeepgramHttpException
import com.deepgram.resources.listen.v1.media.requests.MediaTranscribeRequestOctetStream
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeRequestDiarizeModel
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeRequestModel
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeResponse
import com.deepgram.types.ListenV1AcceptedResponse
import com.deepgram.types.ListenV1Response
import org.springframework.stereotype.Component

@Component
class DeepgramTranscriptionProvider(
    private val client: DeepgramClient,
    private val properties: TranscriptionProperties,
) : TranscriptionProvider {

    override fun transcribe(audio: ByteArray): TranscriptionResult {
        val request = MediaTranscribeRequestOctetStream.builder()
            .body(audio)
            .model(MediaTranscribeRequestModel.NOVA3)
            .language(properties.language)
//            .diarize(true)
            .diarizeModel(MediaTranscribeRequestDiarizeModel.V1)
            .utterances(true)
            .punctuate(true)
            .smartFormat(true)
            .build()
        val response = try {
            client.listen().v1().media().transcribeFile(request)
        } catch (e: DeepgramHttpException) {
            val status = e.statusCode()
            // 401/402/403 = chave ou saldo: problema da conta, não do áudio — retenta em vez de FAILED em massa
            val retryable = status in RETRYABLE_STATUS || status >= 500
            throw TranscriptionFailedException("Deepgram HTTP $status: ${e.body()}", retryable, e)
        } catch (e: DeepgramApiException) {
            throw TranscriptionFailedException("Falha de comunicacao com a Deepgram: ${e.message}", true, e)
        }
        return response.visit(object : MediaTranscribeResponse.Visitor<TranscriptionResult> {
            override fun visit(value: ListenV1Response): TranscriptionResult =
                DeepgramResponseMapper.toResult(value, properties.language)

            override fun visit(value: ListenV1AcceptedResponse): TranscriptionResult =
                throw TranscriptionFailedException("Deepgram respondeu em modo callback sem callback configurado", false)
        })
    }

    private companion object {
        val RETRYABLE_STATUS = setOf(401, 402, 403, 408, 429)
    }
}
