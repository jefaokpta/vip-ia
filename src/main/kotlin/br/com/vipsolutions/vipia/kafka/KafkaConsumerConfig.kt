package br.com.vipsolutions.vipia.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

/**
 * O valor é consumido como String e convertido pelo IaCdrMessageParser: assim uma mensagem malformada é só
 * logada e descartada, sem depender de ErrorHandlingDeserializer.
 */
@EnableKafka
@Configuration
class KafkaConsumerConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val serverUrl: String,
    @param:Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
    @param:Value("\${spring.kafka.consumer.client-id}") private val clientId: String,
    private val kafkaSecurity: KafkaSecurityConfig,
) {

    @Bean
    fun iaCdrConsumerFactory(): ConsumerFactory<String, String> {
        val configs: Map<String, Any> = kafkaSecurity.saslConfigs() + mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to serverUrl,
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.CLIENT_ID_CONFIG to clientId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        )
        return DefaultKafkaConsumerFactory(configs)
    }

    @Bean
    fun iaCdrListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.setConsumerFactory(iaCdrConsumerFactory())
        factory.setCommonErrorHandler(IaCdrErrorHandling.errorHandler())
        return factory
    }
}
