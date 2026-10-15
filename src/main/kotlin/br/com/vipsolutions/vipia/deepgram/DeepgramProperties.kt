package br.com.vipsolutions.vipia.deepgram

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "deepgram")
data class DeepgramProperties(
    val apiKey: String,
    /** Timeout da requisição de transcrição (gravações longas demoram). */
    val timeoutSeconds: Int,
)
