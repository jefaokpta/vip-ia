package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.support.FakeRecordingStorage
import br.com.vipsolutions.vipia.support.FakeTranscriptionProvider
import br.com.vipsolutions.vipia.support.Fixtures
import br.com.vipsolutions.vipia.support.Fixtures.NOW
import br.com.vipsolutions.vipia.support.IntegrationTest
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionFailedException
import br.com.vipsolutions.vipia.usage.UsageProvider
import br.com.vipsolutions.vipia.usage.UsageRecordRepository
import br.com.vipsolutions.vipia.usage.UsageType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Clock
import java.time.Duration
import java.time.ZoneOffset

class TranscriptionProcessorTest : IntegrationTest() {

    @Autowired lateinit var repository: TranscriptionRepository
    @Autowired lateinit var utteranceRepository: TranscriptionUtteranceRepository
    @Autowired lateinit var usageRepository: UsageRecordRepository
    @Autowired lateinit var completionService: TranscriptionCompletionService
    @Autowired lateinit var properties: TranscriptionProperties

    private val storage = FakeRecordingStorage()
    private val provider = FakeTranscriptionProvider()
    private lateinit var processor: TranscriptionProcessor

    @BeforeEach
    fun setUp() {
        processor = TranscriptionProcessor(
            repository = repository,
            storage = storage,
            provider = provider,
            completionService = completionService,
            retryPolicy = RetryPolicy(properties),
            properties = properties,
            clock = Clock.fixed(NOW, ZoneOffset.UTC),
        )
    }

    private fun givenAudio(task: TranscriptionEntity) {
        storage.files["${task.companyId}/${task.recordName}"] = byteArrayOf(1, 2, 3)
    }

    @Test
    fun `transcricao bem sucedida grava texto, falas e consumo`() {
        val task = repository.save(Fixtures.task())
        givenAudio(task)

        assertEquals(1, processor.processDueTasks())

        val done = repository.findById(task.id!!).get()
        assertEquals(TranscriptionStatus.DONE, done.status)
        assertEquals("Olá, bom dia. Bom dia, em que posso ajudar?", done.transcript)
        assertEquals(42.5, done.audioSeconds)
        assertEquals("req-123", done.deepgramRequestId)
        assertEquals(2, utteranceRepository.findByTranscriptionIdOrderByStartSeconds(task.id!!).size)
        val usage = usageRepository.findByReferenceIdAndUsageType(task.id!!, UsageType.TRANSCRIPTION).single()
        assertEquals(UsageProvider.DEEPGRAM, usage.provider)
        assertEquals(42.5, usage.audioSeconds)
        assertEquals("100023", usage.companyId)
        assertNull(usage.cost)
    }

    @Test
    fun `transcricao vazia conclui como DONE`() {
        val task = repository.save(Fixtures.task())
        givenAudio(task)
        provider.result = FakeTranscriptionProvider.sampleResult().copy(transcript = "", utterances = emptyList())

        processor.processDueTasks()

        val done = repository.findById(task.id!!).get()
        assertEquals(TranscriptionStatus.DONE, done.status)
        assertEquals("", done.transcript)
    }

    @Test
    fun `gravacao ausente vira RETRY com backoff`() {
        val task = repository.save(Fixtures.task())

        processor.processDueTasks()

        val retry = repository.findById(task.id!!).get()
        assertEquals(TranscriptionStatus.RETRY, retry.status)
        assertEquals(1, retry.attempts)
        assertEquals(NOW.plus(Duration.ofMinutes(1)), retry.nextAttemptAt)
        assertTrue(retry.lastError!!.contains("recordings/100023/"))
        assertEquals(0, provider.calls)
    }

    @Test
    fun `gravacao ausente na ultima tentativa vira FAILED`() {
        val task = repository.save(Fixtures.task(status = TranscriptionStatus.RETRY, attempts = 5))

        processor.processDueTasks()

        val failed = repository.findById(task.id!!).get()
        assertEquals(TranscriptionStatus.FAILED, failed.status)
        assertEquals(6, failed.attempts)
    }

    @Test
    fun `erro nao retentavel vira FAILED na primeira tentativa`() {
        val task = repository.save(Fixtures.task())
        givenAudio(task)
        provider.error = TranscriptionFailedException("Deepgram HTTP 400", retryable = false)

        processor.processDueTasks()

        val failed = repository.findById(task.id!!).get()
        assertEquals(TranscriptionStatus.FAILED, failed.status)
        assertEquals(1, failed.attempts)
        assertEquals("Deepgram HTTP 400", failed.lastError)
    }

    @Test
    fun `erro inesperado e tratado como retentavel`() {
        val task = repository.save(Fixtures.task())
        givenAudio(task)
        provider.error = IllegalStateException("bug")

        processor.processDueTasks()

        assertEquals(TranscriptionStatus.RETRY, repository.findById(task.id!!).get().status)
    }

    @Test
    fun `Error fora da hierarquia de Exception tambem vira RETRY`() {
        val task = repository.save(Fixtures.task())
        givenAudio(task)
        provider.error = AssertionError("erro grave")

        processor.processDueTasks()

        assertEquals(TranscriptionStatus.RETRY, repository.findById(task.id!!).get().status)
    }

    @Test
    fun `tarefa presa em PROCESSING alem do timeout volta para a fila e e processada`() {
        val stuckSince = NOW.minus(properties.stuckTimeout).minusSeconds(1)
        val stuck = repository.save(Fixtures.task(status = TranscriptionStatus.PROCESSING).copy(updatedAt = stuckSince))
        val recent = repository.save(Fixtures.task(recordName = "recente", status = TranscriptionStatus.PROCESSING))
        givenAudio(stuck)

        processor.processDueTasks()

        repository.findById(stuck.id!!).get().let {
            assertEquals(TranscriptionStatus.DONE, it.status)
            assertEquals(1, it.attempts)
        }
        assertEquals(TranscriptionStatus.PROCESSING, repository.findById(recent.id!!).get().status)
    }

    @Test
    fun `tarefa ja reivindicada por outro nao e processada`() {
        val task = repository.save(Fixtures.task())
        givenAudio(task)
        repository.claim(task.id!!, NOW)

        processor.processOne(task)

        assertEquals(0, provider.calls)
        assertEquals(TranscriptionStatus.PROCESSING, repository.findById(task.id!!).get().status)
    }

    @Test
    fun `tarefa futura nao e processada`() {
        repository.save(Fixtures.task(nextAttemptAt = NOW.plusSeconds(60)))

        assertEquals(0, processor.processDueTasks())
        assertEquals(0, provider.calls)
    }

    @Test
    fun `processa varias tarefas do lote`() {
        val tasks = (1..5).map { repository.save(Fixtures.task(recordName = "rec-$it")) }
        tasks.forEach(::givenAudio)

        assertEquals(5, processor.processDueTasks())

        assertTrue(repository.findAll().all { it.status == TranscriptionStatus.DONE })
        assertEquals(5, usageRepository.count())
    }

}
