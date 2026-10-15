package br.com.vipsolutions.vipia.transcription

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class RetryPolicyTest {

    private val policy = RetryPolicy(
        TranscriptionProperties(Duration.ofSeconds(10), 20, 4, 6, Duration.ofMinutes(1), "pt-BR")
    )
    private val now = Instant.parse("2026-10-15T12:00:00Z")

    @Test
    fun `backoff exponencial a partir da base`() {
        assertEquals(now.plus(Duration.ofMinutes(1)), policy.nextAttemptAt(1, now))
        assertEquals(now.plus(Duration.ofMinutes(2)), policy.nextAttemptAt(2, now))
        assertEquals(now.plus(Duration.ofMinutes(16)), policy.nextAttemptAt(5, now))
    }

    @Test
    fun `desiste ao atingir o maximo de tentativas`() {
        assertFalse(policy.shouldGiveUp(5))
        assertTrue(policy.shouldGiveUp(6))
    }
}
