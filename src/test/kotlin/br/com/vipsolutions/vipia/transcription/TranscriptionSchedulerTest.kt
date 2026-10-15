package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.support.IntegrationTest
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext

class TranscriptionSchedulerTest : IntegrationTest() {

    @Autowired lateinit var context: ApplicationContext

    @Test
    fun `scheduler nao e registrado quando vip-ia scheduling enabled e false`() {
        assertTrue(context.getBeansOfType(TranscriptionScheduler::class.java).isEmpty())
    }

    @Test
    fun `run delega ao processador e engole excecoes`() {
        val processor = mockk<TranscriptionProcessor>()
        every { processor.processDueTasks() } throws IllegalStateException("banco fora")

        assertDoesNotThrow { TranscriptionScheduler(processor).run() }
        verify(exactly = 1) { processor.processDueTasks() }
    }
}
