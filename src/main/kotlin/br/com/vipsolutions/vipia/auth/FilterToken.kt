package br.com.vipsolutions.vipia.auth

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

@Component
class FilterToken(private val jwtService: JwtService) : OncePerRequestFilter() {

    private val tokenPrefix = "Bearer "

    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, filterChain: FilterChain) {
        val header = request.getHeader("Authorization")
        if (header != null && header.startsWith(tokenPrefix)) {
            authenticate(header.removePrefix(tokenPrefix))
        }
        filterChain.doFilter(request, response)
    }

    private fun authenticate(token: String) {
        val decoded = jwtService.validate(token) ?: return
        val principal = try {
            UserPrincipal(decoded)
        } catch (e: Exception) {
            // token assinado mas sem os claims esperados: trata como não autenticado (401), não como 500
            return
        }
        SecurityContextHolder.getContext().authentication =
            UsernamePasswordAuthenticationToken(principal, null, principal.roles.map(::SimpleGrantedAuthority))
    }
}
