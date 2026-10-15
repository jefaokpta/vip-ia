package br.com.vipsolutions.vipia.usage

import org.springframework.stereotype.Service
import java.time.Instant

@Service
class UsageService(private val repository: UsageRecordRepository) {

    /** Custo fica NULL: a Deepgram não devolve preço; o relatório financeiro aplica o valor por minuto. */
    fun recordTranscription(companyId: String, transcriptionId: Long, audioSeconds: Double, model: String, now: Instant) {
        repository.save(
            UsageRecordEntity(
                companyId = companyId,
                usageType = UsageType.TRANSCRIPTION,
                referenceId = transcriptionId,
                provider = UsageProvider.DEEPGRAM,
                model = model,
                audioSeconds = audioSeconds,
                createdAt = now,
            )
        )
    }
}
