package br.com.vipsolutions.vipia.recording

/** Origem dos áudios das chamadas. Em produção é o S3; nos testes, um fake em memória. */
interface RecordingStorage {

    /** @throws RecordingNotFoundException se a gravação ainda não existe (o worker sobe depois da CDR). */
    fun download(companyId: String, recordName: String): ByteArray
}
