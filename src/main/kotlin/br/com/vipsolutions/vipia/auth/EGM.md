# auth

## Overview
Autenticação e autorização HTTP do vip-ia. O serviço apenas **valida** JWTs emitidos pelo vip-pabx-manager (mesmo segredo e issuer); não emite tokens. A sessão é stateless e o principal autenticado carrega o `companyId` usado para isolar os dados por empresa.

## Interface
- `SecurityConfig` (`@Configuration`, `@EnableWebSecurity`, `@EnableMethodSecurity`)
  - `securityFilterChain(http: HttpSecurity): SecurityFilterChain`: CSRF desligado, sessão `STATELESS`, `FilterToken` antes de `UsernamePasswordAuthenticationFilter`, falha de autenticação responde `401` via `HttpStatusEntryPoint`.
  - `corsConfigurationSource(): CorsConfigurationSource`: CORS registrado apenas para `/transcriptions/**` (origens `*`, métodos `GET, POST, PUT, DELETE, OPTIONS`).
- `FilterToken` (`OncePerRequestFilter`): lê o header `Authorization: Bearer <token>` e, se válido, popula o `SecurityContextHolder` com `UsernamePasswordAuthenticationToken(UserPrincipal, null, roles)`.
- `JwtService.validate(token: String): DecodedJWT?`: verifica HMAC512 com `auth.jwt.secret` e issuer `"VIP API"`; retorna `null` em `JWTVerificationException`.
- `UserPrincipal(id, name, email, companyId, tenantId, roles, managingCompany?)`: construído a partir de `DecodedJWT` (claims `id`, `name`, `sub`, `companyId`, `tenantId`, `roles`, `managingCompany`).

## Invariants
- Rotas públicas: somente `/actuator/health`, `/actuator/prometheus`, `/error` e qualquer `OPTIONS /**`; todo o resto exige autenticação (`SecurityConfig.securityFilterChain`).
- Token inválido, com assinatura errada ou sem os claims esperados é tratado como não autenticado (`401`), nunca como erro `500` (`FilterToken.authenticate` engole a exceção do construtor de `UserPrincipal`).
- As authorities do Spring são exatamente as strings do claim `roles` (ex.: `ROLE_SAAS_FINANCE`), sem prefixo adicional.
- Em token de gerenciamento (suporte "virando" a empresa), `companyId` já vem com a empresa gerenciada; o código não reinterpreta `managingCompany`.
- O segredo `auth.jwt.secret` (`AUTH_JWT_SECRET`) e o issuer `"VIP API"` precisam ser idênticos aos do vip-pabx-manager.

## Side Effects
- Escreve no `SecurityContextHolder` da thread da requisição (`FilterToken`).
- Nenhum I/O de rede ou disco: validação de JWT é local.

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.auth.*'` (na raiz `vip-ia/`)
