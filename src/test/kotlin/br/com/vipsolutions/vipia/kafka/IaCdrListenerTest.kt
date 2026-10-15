package br.com.vipsolutions.vipia.kafka

import br.com.vipsolutions.vipia.kafka.message.IaCdrMessage
import br.com.vipsolutions.vipia.transcription.TranscriptionTaskService
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import tools.jackson.module.kotlin.jacksonObjectMapper

class IaCdrListenerTest {

    private val taskService = mockk<TranscriptionTaskService>()
    private val listener = IaCdrListener(IaCdrMessageParser(jacksonObjectMapper()), taskService)

    @Test
    fun `payload valido enfileira as gravacoes`() {
        val captured = slot<IaCdrMessage>()
        every { taskService.enqueue(capture(captured)) } returns 1
        val payload = javaClass.getResourceAsStream("/kafka-contracts/ia-cdr-message.json")!!.bufferedReader().readText()

        listener.onMessage(payload)

        assertEquals("1772633126.1880", captured.captured.uniqueId)
    }

    @Test
    fun `payload malformado nao chama o servico`() {
        listener.onMessage("lixo")

        verify(exactly = 0) { taskService.enqueue(any()) }
    }
}
