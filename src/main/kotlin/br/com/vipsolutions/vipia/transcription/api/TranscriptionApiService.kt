package br.com.vipsolutions.vipia.transcription.api

import br.com.vipsolutions.vipia.common.PageResponse
import br.com.vipsolutions.vipia.transcription.TranscriptionEntity
import br.com.vipsolutions.vipia.transcription.TranscriptionRepository
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import br.com.vipsolutions.vipia.transcription.TranscriptionUtteranceRepository
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionDetail
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionFilter
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionSummary
import br.com.vipsolutions.vipia.transcription.api.dto.UtteranceResponse
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.time.Clock

/** Toda consulta recebe o companyId do token: transcrição de outra empresa é tratada como inexistente (404). */
@Service
class TranscriptionApiService(
    private val repository: TranscriptionRepository,
    private val utteranceRepository: TranscriptionUtteranceRepository,
    private val queryRepository: TranscriptionQueryRepository,
    private val clock: Clock,
) {

    fun search(filter: TranscriptionFilter): PageResponse<TranscriptionSummary> = queryRepository.search(filter)

    fun detail(id: Long, companyId: String): TranscriptionDetail =
        toDetail(repository.findByIdAndCompanyId(id, companyId) ?: throw notFound())

    fun byCdr(uniqueId: String, companyId: String): List<TranscriptionDetail> =
        repository.findByCompanyIdAndCdrUniqueIdOrderByRecordName(companyId, uniqueId).map(::toDetail)

    fun retry(id: Long, companyId: String) {
        val task = repository.findByIdAndCompanyId(id, companyId) ?: throw notFound()
        if (task.status != TranscriptionStatus.FAILED) {
            throw ResponseStatusException(HttpStatus.CONFLICT, "So transcricoes FAILED podem ser reprocessadas (atual: ${task.status})")
        }
        repository.resetFailedForRetry(id, companyId, clock.instant())
    }

    private fun toDetail(entity: TranscriptionEntity) = TranscriptionDetail(
        summary = TranscriptionSummary(entity),
        transcript = entity.transcript,
        language = entity.language,
        model = entity.model,
        utterances = utteranceRepository.findByTranscriptionIdOrderByStartSeconds(requireNotNull(entity.id)).map(::UtteranceResponse),
    )

    private fun notFound() = ResponseStatusException(HttpStatus.NOT_FOUND, "Transcricao nao encontrada")
}
