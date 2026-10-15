package br.com.vipsolutions.vipia.transcription

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(prefix = "vip-ia.scheduling", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class TranscriptionScheduler(private val processor: TranscriptionProcessor) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    @Scheduled(fixedDelayString = "\${vip-ia.transcription.poll-interval}")
    fun run() {
        try {
            processor.processDueTasks()
        } catch (e: Exception) {
            logger.error("Erro na varredura da fila de transcricoes", e)
        }
    }
}
