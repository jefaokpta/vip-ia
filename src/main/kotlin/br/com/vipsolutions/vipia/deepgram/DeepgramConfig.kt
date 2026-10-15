package br.com.vipsolutions.vipia.deepgram

import com.deepgram.DeepgramClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class DeepgramConfig(private val properties: DeepgramProperties) {

    /** maxRetries(0): o retry com backoff é feito pelo TranscriptionProcessor, não pelo SDK. */
    @Bean
    fun deepgramClient(): DeepgramClient = DeepgramClient.builder()
        .apiKey(properties.apiKey)
        .timeout(properties.timeoutSeconds)
        .maxRetries(0)
        .build()
}
