package br.com.vipsolutions.vipia.kafka

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import tools.jackson.module.kotlin.jacksonObjectMapper

class IaCdrMessageParserTest {

    private val parser = IaCdrMessageParser(jacksonObjectMapper())

    private fun goldenFile(): String =
        javaClass.getResourceAsStream("/kafka-contracts/ia-cdr-message.json")!!.bufferedReader().readText()

    @Test
    fun `golden file desserializa com todos os campos`() {
        val msg = parser.parse(goldenFile())

        assertNotNull(msg)
        msg!!
        assertEquals("100023", msg.companyId)
        assertEquals("1772633126.1880", msg.uniqueId)
        assertEquals(1772633126000, msg.startTime)
        assertEquals(8, msg.duration)
        assertEquals(2, msg.billableSeconds)
        assertEquals("12_100023", msg.src)
        assertEquals("1132931515", msg.destination)
        assertEquals("12", msg.peer)
        assertEquals("ANSWERED", msg.disposition)
        assertEquals("OUTBOUND", msg.userfield)
        assertEquals(listOf("100023-1772633126.1880-MIXED"), msg.recordNames)
    }

    @Test
    fun `campos extras sao ignorados`() {
        val json = goldenFile().replaceFirst("{", """{"campoNovo": "x", """)

        assertNotNull(parser.parse(json))
    }

    @Test
    fun `recordNames ausente vira lista vazia`() {
        val json = """{"companyId":"1","uniqueId":"u","startTime":1,"duration":1,"billableSeconds":1,
            "src":"s","destination":"d","peer":"p","disposition":"ANSWERED","userfield":"OUTBOUND"}"""

        assertTrue(parser.parse(json)!!.recordNames.isEmpty())
    }

    @Test
    fun `json invalido retorna null`() {
        assertNull(parser.parse("{isso nao e json"))
    }

    @Test
    fun `campo obrigatorio ausente retorna null`() {
        assertNull(parser.parse("""{"uniqueId":"u","startTime":1}"""))
    }

    @Test
    fun `companyId ou uniqueId em branco retorna null`() {
        assertNull(parser.parse(goldenFile().replace("\"100023\",", "\" \",")))
        assertNull(parser.parse(goldenFile().replace("\"1772633126.1880\",", "\"\",")))
    }
}
