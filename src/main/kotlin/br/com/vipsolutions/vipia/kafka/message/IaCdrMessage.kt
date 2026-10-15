package br.com.vipsolutions.vipia.kafka.message

/**
 * CDR republicada pelo vip-pabx-manager no tópico KAFKA_MANAGER_IA_CDR_TOPIC, só para empresas com o módulo de IA.
 * Contrato: src/test/resources/kafka-contracts/ia-cdr-message.json (espelhado no manager).
 *
 * recordNames são os mixedRecord (sem extensão) das channelInteractions da CDR — o callRecords da CDR do
 * worker vem sempre vazio e não é usado.
 */
data class IaCdrMessage(
    val companyId: String,
    val uniqueId: String,
    val startTime: Long,
    val duration: Int,
    val billableSeconds: Int,
    val src: String,
    val destination: String,
    val peer: String,
    val disposition: String,
    val userfield: String,
    val recordNames: List<String> = emptyList(),
)
