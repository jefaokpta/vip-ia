package br.com.vipsolutions.vipia.recording

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException

class S3RecordingStorageTest {

    private val s3Client = mockk<S3Client>()
    private val storage = S3RecordingStorage(s3Client, "recordings-bucket")

    @Test
    fun `baixa do bucket de gravacoes na chave recordings-companyId-recordName mp3`() {
        val request = slot<GetObjectRequest>()
        val audio = byteArrayOf(1, 2, 3)
        every { s3Client.getObjectAsBytes(capture(request)) } returns
            ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), audio)

        val result = storage.download("100023", "100023-1772633126.1880-MIXED")

        assertArrayEquals(audio, result)
        assertEquals("recordings-bucket", request.captured.bucket())
        assertEquals("recordings/100023/100023-1772633126.1880-MIXED.mp3", request.captured.key())
    }

    @Test
    fun `objeto inexistente vira RecordingNotFoundException`() {
        every { s3Client.getObjectAsBytes(any<GetObjectRequest>()) } throws
            NoSuchKeyException.builder().message("The specified key does not exist.").build()

        val error = assertThrows<RecordingNotFoundException> { storage.download("100023", "rec") }
        assertEquals("recordings/100023/rec.mp3", error.key)
    }
}
