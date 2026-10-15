package br.com.vipsolutions.vipia.kafka

import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.springframework.dao.QueryTimeoutException
import org.springframework.jdbc.CannotGetJdbcConnectionException
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.util.backoff.BackOffExecution

class IaCdrErrorHandlingTest {

    @Test
    fun `banco fora do ar retenta sem nunca descartar a CDR`() {
        val error = ListenerExecutionFailedException("listener falhou", CannotGetJdbcConnectionException("sem conexao"))

        val execution = IaCdrErrorHandling.backOffFor(error)!!.start()

        repeat(1_000) { assertNotEquals(BackOffExecution.STOP, execution.nextBackOff()) }
    }

    @Test
    fun `erro transitorio de acesso a dados tambem retenta`() {
        assertNotNull(IaCdrErrorHandling.backOffFor(ListenerExecutionFailedException("x", QueryTimeoutException("lento"))))
    }

    @Test
    fun `outros erros usam o backoff padrao do handler`() {
        assertNull(IaCdrErrorHandling.backOffFor(ListenerExecutionFailedException("x", IllegalStateException("bug"))))
    }
}
