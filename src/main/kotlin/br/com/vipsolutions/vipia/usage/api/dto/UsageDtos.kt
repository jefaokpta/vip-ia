package br.com.vipsolutions.vipia.usage.api.dto

import br.com.vipsolutions.vipia.usage.UsageProvider
import br.com.vipsolutions.vipia.usage.UsageType
import java.math.BigDecimal
import java.time.Instant

data class UsageSummary(
    val provider: UsageProvider,
    val usageType: UsageType,
    val model: String?,
    val records: Long,
    val audioSeconds: Double,
    val inputTokens: Long,
    val outputTokens: Long,
    /** NULL quando o provedor não informa custo (Deepgram): o financeiro aplica o preço por minuto. */
    val cost: BigDecimal?,
)

data class UsageReport(
    val companyId: String,
    val from: Instant,
    val to: Instant,
    val items: List<UsageSummary>,
)
