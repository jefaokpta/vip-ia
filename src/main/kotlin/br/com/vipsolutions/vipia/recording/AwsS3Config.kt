package br.com.vipsolutions.vipia.recording

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

/** Credenciais vêm da cadeia padrão da AWS (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY no .env). */
@Configuration
class AwsS3Config(@param:Value("\${aws.s3.region}") private val region: String) {

    @Bean
    fun s3Client(): S3Client = S3Client.builder()
        .region(Region.of(region))
        .build()
}
