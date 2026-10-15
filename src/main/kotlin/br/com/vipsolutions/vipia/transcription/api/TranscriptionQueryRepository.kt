package br.com.vipsolutions.vipia.transcription.api

import br.com.vipsolutions.vipia.common.PageResponse
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionFilter
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionSummary
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp

/** Listagem com filtros opcionais: SQL montado à mão porque os filtros são dinâmicos. */
@Repository
class TranscriptionQueryRepository(private val jdbc: NamedParameterJdbcTemplate) {

    private val rowMapper = RowMapper { rs, _ ->
        TranscriptionSummary(
            id = rs.getLong("id"),
            cdrUniqueId = rs.getString("cdr_unique_id"),
            recordName = rs.getString("record_name"),
            status = TranscriptionStatus.valueOf(rs.getString("status")),
            callStartTime = rs.getTimestamp("call_start_time").toInstant(),
            src = rs.getString("src"),
            destination = rs.getString("destination"),
            peer = rs.getString("peer"),
            disposition = rs.getString("disposition"),
            callDuration = rs.getInt("call_duration"),
            audioSeconds = rs.getDouble("audio_seconds").takeUnless { rs.wasNull() },
            attempts = rs.getInt("attempts"),
            lastError = rs.getString("last_error"),
        )
    }

    fun search(filter: TranscriptionFilter): PageResponse<TranscriptionSummary> {
        val params = MapSqlParameterSource("companyId", filter.companyId)
        val where = mutableListOf("company_id = :companyId")
        filter.from?.let { where += "call_start_time >= :from"; params.addValue("from", Timestamp.from(it)) }
        filter.to?.let { where += "call_start_time < :to"; params.addValue("to", Timestamp.from(it)) }
        filter.src?.let { where += "src = :src"; params.addValue("src", it) }
        filter.destination?.let { where += "destination = :destination"; params.addValue("destination", it) }
        filter.peer?.let { where += "peer = :peer"; params.addValue("peer", it) }
        filter.status?.let { where += "status = :status"; params.addValue("status", it.name) }
        val whereClause = where.joinToString(" AND ")

        val total = jdbc.queryForObject("SELECT COUNT(*) FROM transcription WHERE $whereClause", params, Long::class.java) ?: 0L
        params.addValue("limit", filter.size).addValue("offset", filter.page.toLong() * filter.size)
        val content = jdbc.query(
            """SELECT id, cdr_unique_id, record_name, status, call_start_time, src, destination, peer, disposition,
                      call_duration, audio_seconds, attempts, last_error
               FROM transcription WHERE $whereClause
               ORDER BY call_start_time DESC, id DESC
               LIMIT :limit OFFSET :offset""",
            params,
            rowMapper,
        )
        return PageResponse(content, filter.page, filter.size, total)
    }
}
