package br.com.vipsolutions.vipia.transcription.api.dto

import br.com.vipsolutions.vipia.transcription.TranscriptionEntity
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import br.com.vipsolutions.vipia.transcription.TranscriptionUtteranceEntity
import java.time.Instant

data class TranscriptionFilter(
    val companyId: String,
    val from: Instant?,
    val to: Instant?,
    val src: String?,
    val destination: String?,
    val peer: String?,
    val status: TranscriptionStatus?,
    val page: Int,
    val size: Int,
)

data class TranscriptionSummary(
    val id: Long,
    val cdrUniqueId: String,
    val recordName: String,
    val status: TranscriptionStatus,
    val callStartTime: Instant,
    val src: String,
    val destination: String,
    val peer: String,
    val disposition: String,
    val callDuration: Int,
    val audioSeconds: Double?,
    val attempts: Int,
    val lastError: String?,
) {
    constructor(entity: TranscriptionEntity) : this(
        id = requireNotNull(entity.id),
        cdrUniqueId = entity.cdrUniqueId,
        recordName = entity.recordName,
        status = entity.status,
        callStartTime = entity.callStartTime,
        src = entity.src,
        destination = entity.destination,
        peer = entity.peer,
        disposition = entity.disposition,
        callDuration = entity.callDuration,
        audioSeconds = entity.audioSeconds,
        attempts = entity.attempts,
        lastError = entity.lastError,
    )
}

data class UtteranceResponse(
    val speaker: Int,
    val startSeconds: Double,
    val endSeconds: Double,
    val content: String,
    val confidence: Double?,
) {
    constructor(entity: TranscriptionUtteranceEntity) : this(
        speaker = entity.speaker,
        startSeconds = entity.startSeconds,
        endSeconds = entity.endSeconds,
        content = entity.content,
        confidence = entity.confidence,
    )
}

data class TranscriptionDetail(
    val summary: TranscriptionSummary,
    val transcript: String?,
    val language: String?,
    val model: String?,
    val utterances: List<UtteranceResponse>,
)
