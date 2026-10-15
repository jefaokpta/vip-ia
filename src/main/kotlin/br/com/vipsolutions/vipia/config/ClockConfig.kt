package br.com.vipsolutions.vipia.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

/**
 * Relógio único da aplicação: injetado em tudo que calcula horários (backoff, created_at) para os testes
 * poderem usar um Clock fixo.
 */
@Configuration
class ClockConfig {

    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
