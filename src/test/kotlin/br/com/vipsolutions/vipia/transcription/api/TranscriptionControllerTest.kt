package br.com.vipsolutions.vipia.transcription.api

import br.com.vipsolutions.vipia.support.Fixtures
import br.com.vipsolutions.vipia.support.IntegrationTest
import br.com.vipsolutions.vipia.support.TestTokens
import br.com.vipsolutions.vipia.transcription.TranscriptionRepository
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import br.com.vipsolutions.vipia.transcription.TranscriptionUtteranceEntity
import br.com.vipsolutions.vipia.transcription.TranscriptionUtteranceRepository
import org.hamcrest.Matchers.hasSize
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.time.Instant

class TranscriptionControllerTest : IntegrationTest() {

    @Autowired lateinit var repository: TranscriptionRepository
    @Autowired lateinit var utteranceRepository: TranscriptionUtteranceRepository

    private fun doneTask(recordName: String, companyId: String = "100023", start: Instant = Instant.parse("2026-10-14T15:30:00Z"), peer: String = "12"): Long {
        val id = repository.save(Fixtures.task(companyId = companyId, recordName = recordName, callStartTime = start, peer = peer)).id!!
        repository.markDone(id, 42.5, "pt-BR", "nova-3", "req-$id", "Olá, bom dia.", Fixtures.NOW)
        utteranceRepository.save(TranscriptionUtteranceEntity(transcriptionId = id, speaker = 0, startSeconds = 0.5, endSeconds = 1.75, content = "Olá, bom dia.", confidence = 0.97))
        return id
    }

    @Test
    fun `lista so as transcricoes da empresa do token, mais recentes primeiro`() {
        doneTask("a", start = Instant.parse("2026-10-14T10:00:00Z"))
        doneTask("b", start = Instant.parse("2026-10-14T11:00:00Z"))
        doneTask("c", companyId = "999999")

        mockMvc.perform(get("/transcriptions").header("Authorization", TestTokens.bearer(companyId = "100023")))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.content", hasSize<Any>(2)))
            .andExpect(jsonPath("$.content[0].recordName").value("b"))
            .andExpect(jsonPath("$.totalElements").value(2))
            .andExpect(jsonPath("$.page").value(0))
    }

    @Test
    fun `filtra por periodo, peer e status e pagina`() {
        doneTask("a", start = Instant.parse("2026-10-01T10:00:00Z"), peer = "12")
        doneTask("b", start = Instant.parse("2026-10-10T10:00:00Z"), peer = "12")
        doneTask("c", start = Instant.parse("2026-10-10T11:00:00Z"), peer = "13")
        repository.save(Fixtures.task(recordName = "d", status = TranscriptionStatus.FAILED, callStartTime = Instant.parse("2026-10-10T12:00:00Z")))

        mockMvc.perform(
            get("/transcriptions")
                .param("from", "2026-10-05T00:00:00Z")
                .param("to", "2026-10-11T00:00:00Z")
                .param("peer", "12")
                .param("status", "DONE")
                .header("Authorization", TestTokens.bearer())
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.content", hasSize<Any>(1)))
            .andExpect(jsonPath("$.content[0].recordName").value("b"))

        mockMvc.perform(get("/transcriptions").param("size", "1").param("page", "1").header("Authorization", TestTokens.bearer()))
            .andExpect(jsonPath("$.content", hasSize<Any>(1)))
            .andExpect(jsonPath("$.totalElements").value(4))
            .andExpect(jsonPath("$.size").value(1))
    }

    @Test
    fun `detalhe traz texto e falas`() {
        val id = doneTask("a")

        mockMvc.perform(get("/transcriptions/$id").header("Authorization", TestTokens.bearer()))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.summary.id").value(id))
            .andExpect(jsonPath("$.summary.status").value("DONE"))
            .andExpect(jsonPath("$.transcript").value("Olá, bom dia."))
            .andExpect(jsonPath("$.utterances[0].speaker").value(0))
            .andExpect(jsonPath("$.utterances[0].content").value("Olá, bom dia."))
    }

    @Test
    fun `transcricao de outra empresa retorna 404`() {
        val id = doneTask("a", companyId = "999999")

        mockMvc.perform(get("/transcriptions/$id").header("Authorization", TestTokens.bearer(companyId = "100023")))
            .andExpect(status().isNotFound)
        mockMvc.perform(get("/transcriptions/by-cdr/1772633126.1880").header("Authorization", TestTokens.bearer(companyId = "100023")))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$", hasSize<Any>(0)))
        mockMvc.perform(post("/transcriptions/$id/retry").header("Authorization", TestTokens.bearer(companyId = "100023")))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `by-cdr traz todas as gravacoes da chamada`() {
        doneTask("rec-1")
        doneTask("rec-2")

        mockMvc.perform(get("/transcriptions/by-cdr/1772633126.1880").header("Authorization", TestTokens.bearer()))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$", hasSize<Any>(2)))
            .andExpect(jsonPath("$[0].summary.recordName").value("rec-1"))
    }

    @Test
    fun `retry reabre FAILED e recusa outros estados`() {
        val failed = repository.save(Fixtures.task(recordName = "f", status = TranscriptionStatus.FAILED, attempts = 6)).id!!
        val done = doneTask("d")

        mockMvc.perform(post("/transcriptions/$failed/retry").header("Authorization", TestTokens.bearer()))
            .andExpect(status().isNoContent)
        assertEquals(TranscriptionStatus.PENDING, repository.findById(failed).get().status)

        mockMvc.perform(post("/transcriptions/$done/retry").header("Authorization", TestTokens.bearer()))
            .andExpect(status().isConflict)
    }

    @Test
    fun `sem token retorna 401`() {
        mockMvc.perform(get("/transcriptions")).andExpect(status().isUnauthorized)
    }
}
