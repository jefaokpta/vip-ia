package br.com.vipsolutions.vipia.usage.api

import br.com.vipsolutions.vipia.support.IntegrationTest
import br.com.vipsolutions.vipia.support.TestTokens
import br.com.vipsolutions.vipia.usage.UsageProvider
import br.com.vipsolutions.vipia.usage.UsageRecordEntity
import br.com.vipsolutions.vipia.usage.UsageRecordRepository
import br.com.vipsolutions.vipia.usage.UsageType
import org.hamcrest.Matchers.hasSize
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.time.Instant

class UsageControllerTest : IntegrationTest() {

    @Autowired lateinit var usageRepository: UsageRecordRepository

    private fun usage(companyId: String, seconds: Double, at: String) = usageRepository.save(
        UsageRecordEntity(
            companyId = companyId,
            usageType = UsageType.TRANSCRIPTION,
            referenceId = 1,
            provider = UsageProvider.DEEPGRAM,
            model = "nova-3",
            audioSeconds = seconds,
            createdAt = Instant.parse(at),
        )
    )

    private fun request() = get("/internal/usage")
        .param("companyId", "100023")
        .param("from", "2026-10-01T00:00:00Z")
        .param("to", "2026-11-01T00:00:00Z")

    @Test
    fun `financeiro da plataforma ve o consumo agregado do periodo`() {
        usage("100023", 60.0, "2026-10-05T10:00:00Z")
        usage("100023", 30.5, "2026-10-20T10:00:00Z")
        usage("100023", 99.0, "2026-11-01T00:00:00Z") // fora: to é exclusivo
        usage("999999", 10.0, "2026-10-05T10:00:00Z") // outra empresa

        mockMvc.perform(request().header("Authorization", TestTokens.bearer(roles = listOf("ROLE_SAAS_FINANCE"))))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.companyId").value("100023"))
            .andExpect(jsonPath("$.items", hasSize<Any>(1)))
            .andExpect(jsonPath("$.items[0].provider").value("DEEPGRAM"))
            .andExpect(jsonPath("$.items[0].usageType").value("TRANSCRIPTION"))
            .andExpect(jsonPath("$.items[0].model").value("nova-3"))
            .andExpect(jsonPath("$.items[0].records").value(2))
            .andExpect(jsonPath("$.items[0].audioSeconds").value(90.5))
    }

    @Test
    fun `usuario de empresa nao acessa o relatorio`() {
        mockMvc.perform(request().header("Authorization", TestTokens.bearer(roles = listOf("ROLE_COMPANY_ADMIN"))))
            .andExpect(status().isForbidden)
    }

    @Test
    fun `sem token retorna 401`() {
        mockMvc.perform(request()).andExpect(status().isUnauthorized)
    }
}
