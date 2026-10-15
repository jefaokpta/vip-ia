package br.com.vipsolutions.vipia.transcription

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

/** Uma fala contínua de um locutor (diarização da Deepgram). */
@Table("transcription_utterance")
data class TranscriptionUtteranceEntity(
    @Id
    val id: Long? = null,
    val transcriptionId: Long,
    val speaker: Int,
    val startSeconds: Double,
    val endSeconds: Double,
    val content: String,
    val confidence: Double?,
)
