package br.com.vipsolutions.vipia.kafka

import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.RecoverableDataAccessException
import org.springframework.dao.TransientDataAccessException
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.ExponentialBackOff

/**
 * O padrão do Spring Kafka tenta 10 vezes sem intervalo e depois pula a mensagem: com o MySQL fora por poucos
 * segundos, a CDR seria perdida. Falha de infraestrutura do banco retenta para sempre (com backoff, até 1min
 * entre tentativas), segurando a partição até o banco voltar; o resto segue o padrão.
 */
object IaCdrErrorHandling {

    fun errorHandler(): DefaultErrorHandler = DefaultErrorHandler().apply {
        setBackOffFunction { _, error -> backOffFor(error) }
    }

    /** null = usa o backoff padrão do handler. */
    fun backOffFor(error: Exception): BackOff? =
        if (generateSequence<Throwable>(error) { it.cause }.any(::isDatabaseUnavailable)) {
            ExponentialBackOff(1_000, 2.0).apply { maxInterval = 60_000 }
        } else {
            null
        }

    private fun isDatabaseUnavailable(error: Throwable) =
        error is TransientDataAccessException ||
            error is DataAccessResourceFailureException ||
            error is RecoverableDataAccessException
}
