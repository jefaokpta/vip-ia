package br.com.vipsolutions.vipia.auth

import com.auth0.jwt.interfaces.DecodedJWT

/**
 * Usuário autenticado, extraído do JWT do manager. Em token de gerenciamento (suporte "virando" a empresa),
 * companyId já vem com a empresa gerenciada.
 */
data class UserPrincipal(
    val id: Long,
    val name: String,
    val email: String,
    val companyId: String,
    val tenantId: Long,
    val roles: List<String>,
    val managingCompany: Map<String, Any>? = null,
) {

    constructor(decodedJWT: DecodedJWT) : this(
        id = decodedJWT.getClaim("id").asLong(),
        name = decodedJWT.getClaim("name").asString(),
        email = decodedJWT.subject,
        companyId = decodedJWT.getClaim("companyId").asString(),
        tenantId = decodedJWT.getClaim("tenantId").asLong(),
        roles = decodedJWT.getClaim("roles").asList(String::class.java),
        managingCompany = decodedJWT.getClaim("managingCompany").let { if (it.isMissing || it.isNull) null else it.asMap() },
    )
}
