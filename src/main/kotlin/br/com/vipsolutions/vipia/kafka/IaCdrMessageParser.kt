package br.com.vipsolutions.vipia.kafka

import br.com.vipsolutions.vipia.kafka.message.IaCdrMessage
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import tools.jackson.core.JacksonException
import tools.jackson.databind.ObjectMapper

/**
 * Converte o payload cru do Kafka. Mensagem inválida é logada e descartada (retorna null) para não travar
 * o consumo da partição.
 */
@Component
class IaCdrMessageParser(private val objectMapper: ObjectMapper) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun parse(payload: String): IaCdrMessage? {
        val message = try {
            objectMapper.readValue(payload, IaCdrMessage::class.java)
        } catch (e: JacksonException) {
            logger.warn("Mensagem de CDR invalida descartada: ${e.originalMessage} | payload=${payload.take(500)}")
            return null
        }
        if (message.companyId.isBlank() || message.uniqueId.isBlank()) {
            logger.warn("Mensagem de CDR sem companyId/uniqueId descartada | payload=${payload.take(500)}")
            return null
        }
        return message
    }
}
