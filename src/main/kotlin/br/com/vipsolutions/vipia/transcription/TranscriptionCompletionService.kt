package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.transcription.provider.TranscriptionResult
import br.com.vipsolutions.vipia.usage.UsageService
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Clock

/** Grava resultado + falas + consumo numa única transação: ou tudo, ou nada. */
@Service
class TranscriptionCompletionService(
    private val repository: TranscriptionRepository,
    private val utteranceRepository: TranscriptionUtteranceRepository,
    private val usageService: UsageService,
    private val clock: Clock,
) {

    @Transactional
    fun complete(task: TranscriptionEntity, result: TranscriptionResult) {
        val id = requireNotNull(task.id) { "Tarefa sem id" }
        val now = clock.instant()
        repository.markDone(id, result.audioSeconds, result.language, result.model, result.requestId, result.transcript, now)
        utteranceRepository.deleteByTranscriptionId(id)
        utteranceRepository.saveAll(
            result.utterances.map {
                TranscriptionUtteranceEntity(
                    transcriptionId = id,
                    speaker = it.speaker,
                    startSeconds = it.startSeconds,
                    endSeconds = it.endSeconds,
                    content = it.content,
                    confidence = it.confidence,
                )
            }
        )
        usageService.recordTranscription(task.companyId, id, result.audioSeconds, result.model, now)
    }
}
