package br.com.vipsolutions.vipia.transcription

import org.springframework.data.jdbc.repository.query.Modifying
import org.springframework.data.jdbc.repository.query.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param
import java.time.Instant

interface TranscriptionRepository : CrudRepository<TranscriptionEntity, Long> {

    fun existsByCompanyIdAndCdrUniqueIdAndRecordName(companyId: String, cdrUniqueId: String, recordName: String): Boolean

    fun findByIdAndCompanyId(id: Long, companyId: String): TranscriptionEntity?

    fun findByCompanyIdAndCdrUniqueIdOrderByRecordName(companyId: String, cdrUniqueId: String): List<TranscriptionEntity>

    @Query(
        """SELECT * FROM transcription
           WHERE status IN ('PENDING', 'RETRY') AND next_attempt_at <= :now
           ORDER BY next_attempt_at
           LIMIT :limit"""
    )
    fun findDue(@Param("now") now: Instant, @Param("limit") limit: Int): List<TranscriptionEntity>

    /** Reivindica a tarefa de forma atômica: retorna 1 só para quem conseguiu mudar para PROCESSING. */
    @Modifying
    @Query("UPDATE transcription SET status = 'PROCESSING', updated_at = :now WHERE id = :id AND status IN ('PENDING', 'RETRY')")
    fun claim(@Param("id") id: Long, @Param("now") now: Instant): Int

    @Modifying
    @Query(
        """UPDATE transcription
           SET status = 'RETRY', attempts = :attempts, next_attempt_at = :nextAttemptAt, last_error = :error, updated_at = :now
           WHERE id = :id"""
    )
    fun markRetry(
        @Param("id") id: Long,
        @Param("attempts") attempts: Int,
        @Param("nextAttemptAt") nextAttemptAt: Instant,
        @Param("error") error: String,
        @Param("now") now: Instant,
    ): Int

    @Modifying
    @Query("UPDATE transcription SET status = 'FAILED', attempts = :attempts, last_error = :error, updated_at = :now WHERE id = :id")
    fun markFailed(
        @Param("id") id: Long,
        @Param("attempts") attempts: Int,
        @Param("error") error: String,
        @Param("now") now: Instant,
    ): Int

    @Modifying
    @Query(
        """UPDATE transcription
           SET status = 'DONE', audio_seconds = :audioSeconds, language = :language, model = :model,
               deepgram_request_id = :requestId, transcript = :transcript, last_error = NULL, updated_at = :now
           WHERE id = :id"""
    )
    fun markDone(
        @Param("id") id: Long,
        @Param("audioSeconds") audioSeconds: Double,
        @Param("language") language: String,
        @Param("model") model: String,
        @Param("requestId") requestId: String,
        @Param("transcript") transcript: String,
        @Param("now") now: Instant,
    ): Int

    /** Usado no boot: tarefas que ficaram em PROCESSING (app caiu no meio) voltam para a fila. */
    @Modifying
    @Query("UPDATE transcription SET status = 'RETRY', next_attempt_at = :now, updated_at = :now WHERE status = 'PROCESSING'")
    fun resetStuck(@Param("now") now: Instant): Int

    @Modifying
    @Query(
        """UPDATE transcription
           SET status = 'PENDING', attempts = 0, next_attempt_at = :now, last_error = NULL, updated_at = :now
           WHERE id = :id AND company_id = :companyId AND status = 'FAILED'"""
    )
    fun resetFailedForRetry(@Param("id") id: Long, @Param("companyId") companyId: String, @Param("now") now: Instant): Int
}
