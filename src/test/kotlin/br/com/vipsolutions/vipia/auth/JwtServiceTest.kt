package br.com.vipsolutions.vipia.auth

import br.com.vipsolutions.vipia.support.TestTokens
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Instant

class JwtServiceTest {

    private val service = JwtService(TestTokens.SECRET)

    @Test
    fun `token valido do manager vira UserPrincipal`() {
        val decoded = service.validate(TestTokens.token(companyId = "100023", roles = listOf("ROLE_COMPANY_ADMIN")))

        assertNotNull(decoded)
        val principal = UserPrincipal(decoded!!)
        assertEquals("100023", principal.companyId)
        assertEquals(listOf("ROLE_COMPANY_ADMIN"), principal.roles)
        assertEquals("usuario@teste.com", principal.email)
        assertNull(principal.managingCompany)
    }

    @Test
    fun `token de gerenciamento usa o companyId da empresa gerenciada`() {
        val token = TestTokens.token(companyId = "200000", managingCompany = mapOf("companyId" to "200000", "name" to "Cliente"))

        val principal = UserPrincipal(service.validate(token)!!)

        assertEquals("200000", principal.companyId)
        assertEquals("Cliente", principal.managingCompany!!["name"])
    }

    @Test
    fun `assinatura errada, issuer errado ou expirado sao rejeitados`() {
        assertNull(service.validate(TestTokens.token(secret = "outro-segredo")))
        assertNull(service.validate(TestTokens.token(issuer = "OUTRO")))
        assertNull(service.validate(TestTokens.token(expiresAt = Instant.now().minusSeconds(60))))
        assertNull(service.validate("nao-e-jwt"))
    }
}
