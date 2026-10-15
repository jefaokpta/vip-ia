package br.com.vipsolutions.vipia.transcription

import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Clock

/**
 * No boot, tarefas em PROCESSING são de uma execução que caiu no meio: voltam para RETRY.
 * Premissa: uma única instância do vip-ia por banco (com várias instâncias, isto roubaria tarefas em andamento).
 */
@Component
class StuckTranscriptionRecovery(
    private val repository: TranscriptionRepository,
    private val clock: Clock,
) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun recover(): Int {
        val recovered = repository.resetStuck(clock.instant())
        if (recovered > 0) logger.warn("$recovered transcricao(oes) presa(s) em PROCESSING devolvida(s) para RETRY")
        return recovered
    }
}
