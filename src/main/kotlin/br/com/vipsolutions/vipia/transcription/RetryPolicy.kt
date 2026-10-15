package br.com.vipsolutions.vipia.transcription

import org.springframework.stereotype.Component
import java.time.Instant

/** Backoff exponencial: com base 1min e 6 tentativas (esperas de 1+2+4+8+16min), desiste ~31min após a 1ª falha. */
@Component
class RetryPolicy(private val properties: TranscriptionProperties) {

    /** @param attempts tentativas que já falharam (>= 1). */
    fun nextAttemptAt(attempts: Int, now: Instant): Instant {
        val exponent = (attempts - 1).coerceIn(0, 20)
        return now.plus(properties.retryBaseDelay.multipliedBy(1L shl exponent))
    }

    fun shouldGiveUp(attempts: Int): Boolean = attempts >= properties.maxAttempts
}
