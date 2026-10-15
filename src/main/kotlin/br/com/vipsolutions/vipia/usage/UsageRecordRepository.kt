package br.com.vipsolutions.vipia.usage

import org.springframework.data.repository.CrudRepository

interface UsageRecordRepository : CrudRepository<UsageRecordEntity, Long> {

    fun findByReferenceIdAndUsageType(referenceId: Long, usageType: UsageType): List<UsageRecordEntity>
}
