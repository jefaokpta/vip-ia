package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.kafka.message.IaCdrMessage
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Instant

/**
 * Transforma uma CDR em tarefas de transcrição (uma por gravação). Idempotente: a chave única
 * (company_id, cdr_unique_id, record_name) garante que reentregas do Kafka não dupliquem trabalho nem custo.
 */
@Service
class TranscriptionTaskService(
    private val repository: TranscriptionRepository,
    private val clock: Clock,
) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun enqueue(message: IaCdrMessage): Int {
        val recordNames = message.recordNames.map(String::trim).filter(String::isNotEmpty).distinct()
        if (recordNames.isEmpty()) {
            logger.info("${message.uniqueId} >> CDR sem gravacao, nada a transcrever (empresa ${message.companyId})")
            return 0
        }
        val now = clock.instant()
        return recordNames.count { recordName -> createTask(message, recordName, now) }
    }

    private fun createTask(message: IaCdrMessage, recordName: String, now: Instant): Boolean {
        if (repository.existsByCompanyIdAndCdrUniqueIdAndRecordName(message.companyId, message.uniqueId, recordName)) {
            logger.info("${message.uniqueId} >> gravacao $recordName ja enfileirada, ignorando")
            return false
        }
        return try {
            repository.save(newTask(message, recordName, now))
            true
        } catch (e: DuplicateKeyException) {
            // corrida entre o exists e o insert: outra entrega da mesma mensagem ganhou
            logger.info("${message.uniqueId} >> gravacao $recordName enfileirada em paralelo, ignorando")
            false
        }
    }

    private fun newTask(message: IaCdrMessage, recordName: String, now: Instant) = TranscriptionEntity(
        companyId = message.companyId,
        cdrUniqueId = message.uniqueId,
        recordName = recordName,
        status = TranscriptionStatus.PENDING,
        nextAttemptAt = now,
        callStartTime = Instant.ofEpochMilli(message.startTime),
        src = message.src,
        destination = message.destination,
        peer = message.peer,
        disposition = message.disposition,
        userfield = message.userfield,
        callDuration = message.duration,
        createdAt = now,
        updatedAt = now,
    )
}
