package br.com.vipsolutions.vipia.transcription

import org.springframework.data.jdbc.repository.query.Modifying
import org.springframework.data.jdbc.repository.query.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param

interface TranscriptionUtteranceRepository : CrudRepository<TranscriptionUtteranceEntity, Long> {

    fun findByTranscriptionIdOrderByStartSeconds(transcriptionId: Long): List<TranscriptionUtteranceEntity>

    @Modifying
    @Query("DELETE FROM transcription_utterance WHERE transcription_id = :transcriptionId")
    fun deleteByTranscriptionId(@Param("transcriptionId") transcriptionId: Long): Int
}
