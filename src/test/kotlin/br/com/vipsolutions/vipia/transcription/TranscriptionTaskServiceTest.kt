package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.kafka.message.IaCdrMessage
import br.com.vipsolutions.vipia.support.IntegrationTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Instant

class TranscriptionTaskServiceTest : IntegrationTest() {

    @Autowired
    lateinit var service: TranscriptionTaskService

    @Autowired
    lateinit var repository: TranscriptionRepository

    private fun message(recordNames: List<String>) = IaCdrMessage(
        companyId = "100023",
        uniqueId = "1772633126.1880",
        startTime = 1772633126000,
        duration = 8,
        billableSeconds = 2,
        src = "12_100023",
        destination = "1132931515",
        peer = "12",
        disposition = "ANSWERED",
        userfield = "OUTBOUND",
        recordNames = recordNames,
    )

    @Test
    fun `cria uma tarefa PENDING por gravacao com os metadados da CDR`() {
        val created = service.enqueue(message(listOf("rec-1", "rec-2")))

        assertEquals(2, created)
        val tasks = repository.findByCompanyIdAndCdrUniqueIdOrderByRecordName("100023", "1772633126.1880")
        assertEquals(listOf("rec-1", "rec-2"), tasks.map { it.recordName })
        tasks.forEach {
            assertEquals(TranscriptionStatus.PENDING, it.status)
            assertEquals(0, it.attempts)
            assertEquals(Instant.ofEpochMilli(1772633126000), it.callStartTime)
            assertEquals("12_100023", it.src)
            assertEquals("1132931515", it.destination)
            assertEquals("12", it.peer)
            assertEquals(8, it.callDuration)
        }
    }

    @Test
    fun `mensagem repetida nao duplica tarefas`() {
        service.enqueue(message(listOf("rec-1")))
        val createdAgain = service.enqueue(message(listOf("rec-1", "rec-2")))

        assertEquals(1, createdAgain)
        assertEquals(2, repository.count())
    }

    @Test
    fun `nomes repetidos ou em branco na mesma mensagem sao ignorados`() {
        assertEquals(1, service.enqueue(message(listOf("rec-1", "rec-1", " ", ""))))
    }

    @Test
    fun `CDR sem gravacao nao cria tarefa`() {
        assertEquals(0, service.enqueue(message(emptyList())))
        assertEquals(0, repository.count())
    }
}
