package br.com.vipsolutions.vipia.usage.api

import br.com.vipsolutions.vipia.usage.UsageProvider
import br.com.vipsolutions.vipia.usage.UsageType
import br.com.vipsolutions.vipia.usage.api.dto.UsageSummary
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant

@Repository
class UsageReportRepository(private val jdbc: NamedParameterJdbcTemplate) {

    fun summarize(companyId: String, from: Instant, to: Instant): List<UsageSummary> = jdbc.query(
        """SELECT provider, usage_type, model,
                  COUNT(*) AS records,
                  COALESCE(SUM(audio_seconds), 0) AS audio_seconds,
                  COALESCE(SUM(input_tokens), 0) AS input_tokens,
                  COALESCE(SUM(output_tokens), 0) AS output_tokens,
                  SUM(cost) AS cost
           FROM usage_record
           WHERE company_id = :companyId AND created_at >= :from AND created_at < :to
           GROUP BY provider, usage_type, model
           ORDER BY provider, usage_type, model""",
        MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("from", Timestamp.from(from))
            .addValue("to", Timestamp.from(to)),
    ) { rs, _ ->
        UsageSummary(
            provider = UsageProvider.valueOf(rs.getString("provider")),
            usageType = UsageType.valueOf(rs.getString("usage_type")),
            model = rs.getString("model"),
            records = rs.getLong("records"),
            audioSeconds = rs.getDouble("audio_seconds"),
            inputTokens = rs.getLong("input_tokens"),
            outputTokens = rs.getLong("output_tokens"),
            cost = rs.getBigDecimal("cost"),
        )
    }
}
