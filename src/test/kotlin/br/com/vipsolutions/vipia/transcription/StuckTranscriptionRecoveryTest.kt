package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.support.Fixtures
import br.com.vipsolutions.vipia.support.IntegrationTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

class StuckTranscriptionRecoveryTest : IntegrationTest() {

    @Autowired lateinit var recovery: StuckTranscriptionRecovery
    @Autowired lateinit var repository: TranscriptionRepository

    @Test
    fun `tarefas PROCESSING voltam para RETRY e as demais nao mudam`() {
        val stuck = repository.save(Fixtures.task(recordName = "a", status = TranscriptionStatus.PROCESSING)).id!!
        val done = repository.save(Fixtures.task(recordName = "b", status = TranscriptionStatus.DONE)).id!!

        assertEquals(1, recovery.recover())

        assertEquals(TranscriptionStatus.RETRY, repository.findById(stuck).get().status)
        assertEquals(TranscriptionStatus.DONE, repository.findById(done).get().status)
    }
}
