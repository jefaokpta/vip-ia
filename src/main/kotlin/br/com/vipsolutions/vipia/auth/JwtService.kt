package br.com.vipsolutions.vipia.auth

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.exceptions.JWTVerificationException
import com.auth0.jwt.interfaces.DecodedJWT
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

/** Só valida: quem emite o token é o vip-pabx-manager (mesmo segredo e issuer). */
@Service
class JwtService(@param:Value("\${auth.jwt.secret}") private val secret: String) {

    private val verifier = JWT.require(Algorithm.HMAC512(secret))
        .withIssuer("VIP API")
        .build()

    fun validate(token: String): DecodedJWT? = try {
        verifier.verify(token)
    } catch (e: JWTVerificationException) {
        null
    }
}
