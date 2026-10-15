package br.com.vipsolutions.vipia.auth

import br.com.vipsolutions.vipia.support.IntegrationTest
import br.com.vipsolutions.vipia.support.TestTokens
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

/**
 * Usa uma rota inexistente: sem auth deve dar 401; com token válido passa da segurança e cai no 404.
 */
class SecurityConfigTest : IntegrationTest() {

    @Test
    fun `sem token retorna 401`() {
        mockMvc.perform(get("/rota-inexistente")).andExpect(status().isUnauthorized)
    }

    @Test
    fun `token invalido retorna 401`() {
        mockMvc.perform(get("/rota-inexistente").header("Authorization", "Bearer ${TestTokens.token(secret = "x")}"))
            .andExpect(status().isUnauthorized)
    }

    @Test
    fun `token valido passa pela seguranca`() {
        mockMvc.perform(get("/rota-inexistente").header("Authorization", TestTokens.bearer()))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `actuator continua aberto`() {
        mockMvc.perform(get("/actuator/health")).andExpect(status().isOk)
    }
}
