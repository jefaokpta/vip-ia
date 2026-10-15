package br.com.vipsolutions.vipia.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

/** Desligado nos testes (vip-ia.scheduling.enabled=false) para os processadores só rodarem quando chamados. */
@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "vip-ia.scheduling", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class SchedulingConfig
