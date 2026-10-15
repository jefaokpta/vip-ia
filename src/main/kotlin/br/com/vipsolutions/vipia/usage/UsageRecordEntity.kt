package br.com.vipsolutions.vipia.usage

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

/**
 * Consumo de um provedor externo. A Deepgram não informa custo na resposta: em TRANSCRIPTION o cost fica
 * NULL e o relatório financeiro aplica o preço por minuto sobre audioSeconds.
 */
@Table("usage_record")
data class UsageRecordEntity(
    @Id
    val id: Long? = null,
    val companyId: String,
    val usageType: UsageType,
    val referenceId: Long,
    val provider: UsageProvider,
    val model: String?,
    val audioSeconds: Double? = null,
    val inputTokens: Int? = null,
    val outputTokens: Int? = null,
    val cost: BigDecimal? = null,
    val createdAt: Instant,
)
