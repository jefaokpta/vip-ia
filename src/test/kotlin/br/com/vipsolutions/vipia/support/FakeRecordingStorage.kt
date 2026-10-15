package br.com.vipsolutions.vipia.support

import br.com.vipsolutions.vipia.recording.RecordingNotFoundException
import br.com.vipsolutions.vipia.recording.RecordingStorage

class FakeRecordingStorage : RecordingStorage {

    val files = mutableMapOf<String, ByteArray>()

    override fun download(companyId: String, recordName: String): ByteArray =
        files["$companyId/$recordName"] ?: throw RecordingNotFoundException("recordings/$companyId/$recordName.mp3")
}
