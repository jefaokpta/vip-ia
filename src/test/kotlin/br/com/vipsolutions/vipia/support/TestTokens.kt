package br.com.vipsolutions.vipia.support

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import java.time.Instant

/** Gera tokens no mesmo formato do JWTService do vip-pabx-manager. */
object TestTokens {

    const val SECRET = "test-secret"

    fun token(
        companyId: String = "100023",
        roles: List<String> = listOf("ROLE_COMPANY_USER"),
        secret: String = SECRET,
        issuer: String = "VIP API",
        expiresAt: Instant = Instant.now().plusSeconds(300),
        managingCompany: Map<String, Any>? = null,
    ): String {
        val builder = JWT.create()
            .withIssuer(issuer)
            .withSubject("usuario@teste.com")
            .withClaim("id", 1L)
            .withClaim("name", "Usuario Teste")
            .withClaim("companyId", companyId)
            .withClaim("tenantId", 1L)
            .withClaim("isPasswordCreated", true)
            .withClaim("roles", roles)
            .withExpiresAt(expiresAt)
        managingCompany?.let { builder.withClaim("managingCompany", it) }
        return builder.sign(Algorithm.HMAC512(secret))
    }

    fun bearer(
        companyId: String = "100023",
        roles: List<String> = listOf("ROLE_COMPANY_USER"),
    ): String = "Bearer ${token(companyId = companyId, roles = roles)}"
}
