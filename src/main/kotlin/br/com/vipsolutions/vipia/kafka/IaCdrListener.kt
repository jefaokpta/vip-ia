package br.com.vipsolutions.vipia.kafka

import br.com.vipsolutions.vipia.transcription.TranscriptionTaskService
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * Só persiste as tarefas: a transcrição roda depois no TranscriptionProcessor. Assim o offset é confirmado
 * rápido e a gravação que ainda não chegou ao S3 não trava a partição.
 */
@Component
class IaCdrListener(
    private val parser: IaCdrMessageParser,
    private val taskService: TranscriptionTaskService,
) {

    @KafkaListener(
        containerFactory = "iaCdrListenerContainerFactory",
        topics = ["\${spring.kafka.consumer.ia-cdr-topic}"],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "\${vip-ia.kafka.listener.auto-startup:true}",
    )
    fun onMessage(payload: String) {
        val message = parser.parse(payload) ?: return
        taskService.enqueue(message)
    }
}
