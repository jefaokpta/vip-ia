package br.com.vipsolutions.vipia.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * Propriedades de autenticação SASL/SCRAM-SHA-512 compartilhadas pelas factories Kafka.
 * As factories usam mapas explícitos, então `spring.kafka.*` de segurança não é aplicado automaticamente.
 */
@Component
class KafkaSecurityConfig(
    @param:Value("\${spring.kafka.sasl.username}") private val username: String,
    @param:Value("\${spring.kafka.sasl.password}") private val password: String,
) {

    init {
        require(username.isNotBlank()) { "KAFKA_SASL_USERNAME não pode ser vazio" }
        require(password.isNotBlank()) { "KAFKA_SASL_PASSWORD não pode ser vazio" }
    }

    fun saslConfigs(): Map<String, Any> = mapOf(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SASL_PLAINTEXT",
        SaslConfigs.SASL_MECHANISM to "SCRAM-SHA-512",
        SaslConfigs.SASL_JAAS_CONFIG to "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"${escape(username)}\" password=\"${escape(password)}\";",
    )

    private fun escape(value: String) = value.replace("\\", "\\\\").replace("\"", "\\\"")
}
