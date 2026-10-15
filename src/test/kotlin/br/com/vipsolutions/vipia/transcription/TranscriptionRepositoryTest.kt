package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.support.Fixtures
import br.com.vipsolutions.vipia.support.Fixtures.NOW
import br.com.vipsolutions.vipia.support.IntegrationTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DuplicateKeyException

class TranscriptionRepositoryTest : IntegrationTest() {

    @Autowired
    lateinit var repository: TranscriptionRepository

    @Autowired
    lateinit var utteranceRepository: TranscriptionUtteranceRepository

    @Test
    fun `salva e le uma tarefa com todos os campos`() {
        val saved = repository.save(Fixtures.task())

        val loaded = repository.findByIdAndCompanyId(saved.id!!, "100023")!!
        assertEquals(TranscriptionStatus.PENDING, loaded.status)
        assertEquals(Fixtures.task().callStartTime, loaded.callStartTime)
        assertEquals("100023-1772633126.1880-MIXED", loaded.recordName)
    }

    @Test
    fun `findByIdAndCompanyId nao retorna tarefa de outra empresa`() {
        val saved = repository.save(Fixtures.task(companyId = "100023"))

        assertNull(repository.findByIdAndCompanyId(saved.id!!, "999999"))
    }

    @Test
    fun `chave unica impede a mesma gravacao duas vezes`() {
        repository.save(Fixtures.task())

        assertThrows<DuplicateKeyException> { repository.save(Fixtures.task()) }
    }

    @Test
    fun `findDue traz so PENDING e RETRY vencidas, em ordem e com limite`() {
        repository.save(Fixtures.task(recordName = "a", nextAttemptAt = NOW.minusSeconds(30)))
        repository.save(Fixtures.task(recordName = "b", status = TranscriptionStatus.RETRY, nextAttemptAt = NOW.minusSeconds(60)))
        repository.save(Fixtures.task(recordName = "c", nextAttemptAt = NOW.plusSeconds(60)))
        repository.save(Fixtures.task(recordName = "d", status = TranscriptionStatus.DONE))
        repository.save(Fixtures.task(recordName = "e", status = TranscriptionStatus.PROCESSING))

        val due = repository.findDue(NOW, 10)
        assertEquals(listOf("b", "a"), due.map { it.recordName })
        assertEquals(1, repository.findDue(NOW, 1).size)
    }

    @Test
    fun `claim so funciona uma vez`() {
        val id = repository.save(Fixtures.task()).id!!

        assertEquals(1, repository.claim(id, NOW))
        assertEquals(0, repository.claim(id, NOW))
        assertEquals(TranscriptionStatus.PROCESSING, repository.findById(id).get().status)
    }

    @Test
    fun `markRetry, markFailed e markDone atualizam estado`() {
        val id = repository.save(Fixtures.task()).id!!

        repository.markRetry(id, 1, NOW.plusSeconds(60), "sem audio", NOW)
        repository.findById(id).get().let {
            assertEquals(TranscriptionStatus.RETRY, it.status)
            assertEquals(1, it.attempts)
            assertEquals(NOW.plusSeconds(60), it.nextAttemptAt)
            assertEquals("sem audio", it.lastError)
        }

        repository.markFailed(id, 2, "erro fatal", NOW)
        assertEquals(TranscriptionStatus.FAILED, repository.findById(id).get().status)

        repository.markDone(id, 42.5, "pt-BR", "nova-3", "req-1", "Olá", NOW)
        repository.findById(id).get().let {
            assertEquals(TranscriptionStatus.DONE, it.status)
            assertEquals(42.5, it.audioSeconds)
            assertEquals("Olá", it.transcript)
            assertNull(it.lastError)
        }
    }

    @Test
    fun `resetStuck devolve PROCESSING para RETRY`() {
        val id = repository.save(Fixtures.task(status = TranscriptionStatus.PROCESSING)).id!!

        assertEquals(1, repository.resetStuck(NOW))
        repository.findById(id).get().let {
            assertEquals(TranscriptionStatus.RETRY, it.status)
            assertEquals(NOW, it.nextAttemptAt)
        }
    }

    @Test
    fun `resetFailedForRetry so reabre FAILED da propria empresa`() {
        val failed = repository.save(Fixtures.task(status = TranscriptionStatus.FAILED, attempts = 6)).id!!
        val done = repository.save(Fixtures.task(recordName = "x", status = TranscriptionStatus.DONE)).id!!

        assertEquals(0, repository.resetFailedForRetry(failed, "999999", NOW))
        assertEquals(0, repository.resetFailedForRetry(done, "100023", NOW))
        assertEquals(1, repository.resetFailedForRetry(failed, "100023", NOW))
        repository.findById(failed).get().let {
            assertEquals(TranscriptionStatus.PENDING, it.status)
            assertEquals(0, it.attempts)
        }
    }

    @Test
    fun `utterances sao salvas, lidas em ordem e apagadas por transcricao`() {
        val id = repository.save(Fixtures.task()).id!!
        utteranceRepository.saveAll(
            listOf(
                TranscriptionUtteranceEntity(transcriptionId = id, speaker = 1, startSeconds = 2.0, endSeconds = 3.0, content = "segunda", confidence = 0.9),
                TranscriptionUtteranceEntity(transcriptionId = id, speaker = 0, startSeconds = 0.5, endSeconds = 1.5, content = "primeira", confidence = null),
            )
        )

        assertEquals(listOf("primeira", "segunda"), utteranceRepository.findByTranscriptionIdOrderByStartSeconds(id).map { it.content })
        assertEquals(2, utteranceRepository.deleteByTranscriptionId(id))
        assertTrue(utteranceRepository.findByTranscriptionIdOrderByStartSeconds(id).isEmpty())
    }
}
