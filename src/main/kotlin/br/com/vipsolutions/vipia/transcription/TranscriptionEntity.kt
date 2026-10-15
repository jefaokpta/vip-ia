package br.com.vipsolutions.vipia.transcription

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant

/**
 * Uma tarefa de transcrição = uma gravação de uma CDR. Também guarda o resultado (texto, duração, modelo).
 * Mudanças de estado são feitas por UPDATEs explícitos no repositório (ver TranscriptionRepository), nunca
 * por save() da entidade inteira, para não sobrescrever campos em corrida.
 */
@Table("transcription")
data class TranscriptionEntity(
    @Id
    val id: Long? = null,
    val companyId: String,
    val cdrUniqueId: String,
    val recordName: String,
    val status: TranscriptionStatus,
    val attempts: Int = 0,
    val nextAttemptAt: Instant,
    val lastError: String? = null,
    val callStartTime: Instant,
    val src: String,
    val destination: String,
    val peer: String,
    val disposition: String,
    val userfield: String,
    val callDuration: Int,
    val audioSeconds: Double? = null,
    val language: String? = null,
    val model: String? = null,
    val deepgramRequestId: String? = null,
    val transcript: String? = null,
    val createdAt: Instant,
    val updatedAt: Instant,
)
