package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.recording.RecordingNotFoundException
import br.com.vipsolutions.vipia.recording.RecordingStorage
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionFailedException
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionProvider
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

/**
 * Consome a fila de transcrições: busca tarefas vencidas, reivindica cada uma atomicamente (claim), baixa o
 * áudio, transcreve e conclui. Falha vira RETRY (com backoff) ou FAILED. Roda cada tarefa numa virtual thread,
 * limitado por maxConcurrency para controlar custo e rate limit da Deepgram.
 */
@Service
class TranscriptionProcessor(
    private val repository: TranscriptionRepository,
    private val storage: RecordingStorage,
    private val provider: TranscriptionProvider,
    private val completionService: TranscriptionCompletionService,
    private val retryPolicy: RetryPolicy,
    private val properties: TranscriptionProperties,
    private val clock: Clock,
) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun processDueTasks(): Int {
        val now = clock.instant()
        val stale = repository.resetStale(now.minus(properties.stuckTimeout), now)
        if (stale > 0) logger.warn("$stale transcricao(oes) presa(s) em PROCESSING devolvida(s) para RETRY")
        val due = repository.findDue(now, properties.batchSize)
        if (due.isEmpty()) return 0
        val permits = Semaphore(properties.maxConcurrency)
        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            due.forEach { task ->
                executor.submit {
                    permits.acquire()
                    try {
                        processOne(task)
                    } catch (e: Throwable) {
                        // sem isso a exceção fica no Future descartado e a tarefa some em PROCESSING sem log
                        logger.error("${task.cdrUniqueId} >> falha ao registrar resultado de ${task.recordName}", e)
                    } finally {
                        permits.release()
                    }
                }
            }
        }
        return due.size
    }

    fun processOne(task: TranscriptionEntity) {
        val id = requireNotNull(task.id) { "Tarefa sem id" }
        if (repository.claim(id, clock.instant()) == 0) return
        try {
            val audio = storage.download(task.companyId, task.recordName)
            val result = provider.transcribe(audio)
            completionService.complete(task, result)
            logger.info("${task.cdrUniqueId} >> gravacao ${task.recordName} transcrita (${result.audioSeconds}s)")
        } catch (e: RecordingNotFoundException) {
            registerFailure(task, e.message ?: "Gravacao nao encontrada", retryable = true)
        } catch (e: TranscriptionFailedException) {
            registerFailure(task, e.message ?: "Falha na transcricao", e.retryable)
        } catch (e: Throwable) {
            logger.error("${task.cdrUniqueId} >> erro inesperado ao transcrever ${task.recordName}", e)
            registerFailure(task, e.message ?: e.javaClass.simpleName, retryable = true)
        }
    }

    private fun registerFailure(task: TranscriptionEntity, error: String, retryable: Boolean) {
        val id = requireNotNull(task.id)
        val attempts = task.attempts + 1
        val now = clock.instant()
        val message = error.take(1000)
        if (!retryable || retryPolicy.shouldGiveUp(attempts)) {
            repository.markFailed(id, attempts, message, now)
            logger.warn("${task.cdrUniqueId} >> transcricao de ${task.recordName} FALHOU apos $attempts tentativa(s): $message")
        } else {
            val next = retryPolicy.nextAttemptAt(attempts, now)
            repository.markRetry(id, attempts, next, message, now)
            logger.info("${task.cdrUniqueId} >> transcricao de ${task.recordName} reagendada para $next: $message")
        }
    }
}
