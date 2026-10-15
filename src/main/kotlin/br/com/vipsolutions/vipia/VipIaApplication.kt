package br.com.vipsolutions.vipia

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class VipIaApplication

fun main(args: Array<String>) {
    runApplication<VipIaApplication>(*args)
}
