package br.com.vipsolutions.vipia.transcription.api

import br.com.vipsolutions.vipia.auth.UserPrincipal
import br.com.vipsolutions.vipia.common.PageResponse
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionDetail
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionFilter
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionSummary
import org.springframework.http.HttpStatus
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

@RestController
@RequestMapping("/transcriptions")
class TranscriptionController(private val service: TranscriptionApiService) {

    @GetMapping
    fun search(
        @AuthenticationPrincipal user: UserPrincipal,
        @RequestParam(required = false) from: Instant?,
        @RequestParam(required = false) to: Instant?,
        @RequestParam(required = false) src: String?,
        @RequestParam(required = false) destination: String?,
        @RequestParam(required = false) peer: String?,
        @RequestParam(required = false) status: TranscriptionStatus?,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int,
    ): PageResponse<TranscriptionSummary> = service.search(
        TranscriptionFilter(
            companyId = user.companyId,
            from = from,
            to = to,
            src = src,
            destination = destination,
            peer = peer,
            status = status,
            page = page.coerceAtLeast(0),
            size = size.coerceIn(1, 100),
        )
    )

    @GetMapping("/{id}")
    fun detail(@AuthenticationPrincipal user: UserPrincipal, @PathVariable id: Long): TranscriptionDetail =
        service.detail(id, user.companyId)

    @GetMapping("/by-cdr/{uniqueId}")
    fun byCdr(@AuthenticationPrincipal user: UserPrincipal, @PathVariable uniqueId: String): List<TranscriptionDetail> =
        service.byCdr(uniqueId, user.companyId)

    @PostMapping("/{id}/retry")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun retry(@AuthenticationPrincipal user: UserPrincipal, @PathVariable id: Long) =
        service.retry(id, user.companyId)
}
