package br.com.vipsolutions.vipia.usage.api

import br.com.vipsolutions.vipia.usage.api.dto.UsageReport
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

/** Consumido pelo relatório financeiro (manager). Roles são cumulativas: SAAS_ADMIN também tem SAAS_FINANCE. */
@RestController
@RequestMapping("/internal/usage")
class UsageController(private val repository: UsageReportRepository) {

    @GetMapping
    @PreAuthorize("hasAuthority('ROLE_SAAS_FINANCE')")
    fun report(
        @RequestParam companyId: String,
        @RequestParam from: Instant,
        @RequestParam to: Instant,
    ): UsageReport = UsageReport(companyId, from, to, repository.summarize(companyId, from, to))
}
