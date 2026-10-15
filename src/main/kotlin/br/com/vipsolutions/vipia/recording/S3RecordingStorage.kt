package br.com.vipsolutions.vipia.recording

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException

/** Lê as gravações que o vip-worker sobe em recordings/{companyId}/{recordName}.mp3. */
@Component
class S3RecordingStorage(
    private val s3Client: S3Client,
    @param:Value("\${aws.s3.recordings.bucket}") private val recordingsBucket: String,
) : RecordingStorage {

    override fun download(companyId: String, recordName: String): ByteArray {
        val key = "recordings/$companyId/$recordName.mp3"
        val request = GetObjectRequest.builder()
            .bucket(recordingsBucket)
            .key(key)
            .build()
        return try {
            s3Client.getObjectAsBytes(request).asByteArray()
        } catch (e: NoSuchKeyException) {
            throw RecordingNotFoundException(key)
        }
    }
}
