package br.com.vipsolutions.vipia.transcription

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "vip-ia.transcription")
data class TranscriptionProperties(
    /** Intervalo entre varreduras da fila. */
    val pollInterval: Duration,
    /** Máximo de tarefas buscadas por varredura. */
    val batchSize: Int,
    /** Máximo de chamadas simultâneas à Deepgram. */
    val maxConcurrency: Int,
    /** Tentativas antes de FAILED. */
    val maxAttempts: Int,
    /** Base do backoff exponencial: base * 2^(tentativas-1). */
    val retryBaseDelay: Duration,
    /** Idioma enviado à Deepgram (BCP-47). */
    val language: String,
    /** Tarefa em PROCESSING sem atualização há mais que isso é considerada presa e volta para RETRY. */
    val stuckTimeout: Duration = Duration.ofMinutes(10),
)
