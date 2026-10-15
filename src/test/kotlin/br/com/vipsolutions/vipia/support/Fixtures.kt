package br.com.vipsolutions.vipia.support

import br.com.vipsolutions.vipia.transcription.TranscriptionEntity
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import java.time.Instant

object Fixtures {

    val NOW: Instant = Instant.parse("2026-10-15T12:00:00Z")

    fun task(
        companyId: String = "100023",
        cdrUniqueId: String = "1772633126.1880",
        recordName: String = "100023-1772633126.1880-MIXED",
        status: TranscriptionStatus = TranscriptionStatus.PENDING,
        attempts: Int = 0,
        nextAttemptAt: Instant = NOW.minusSeconds(1),
        callStartTime: Instant = Instant.parse("2026-10-14T15:30:00Z"),
        src: String = "12_100023",
        destination: String = "1132931515",
        peer: String = "12",
    ) = TranscriptionEntity(
        companyId = companyId,
        cdrUniqueId = cdrUniqueId,
        recordName = recordName,
        status = status,
        attempts = attempts,
        nextAttemptAt = nextAttemptAt,
        callStartTime = callStartTime,
        src = src,
        destination = destination,
        peer = peer,
        disposition = "ANSWERED",
        userfield = "OUTBOUND",
        callDuration = 42,
        createdAt = NOW,
        updatedAt = NOW,
    )
}
