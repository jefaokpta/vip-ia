package br.com.vipsolutions.vipia.support

import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.web.servlet.MockMvc

/**
 * Base dos testes que usam o contexto Spring + H2. Um único contexto é compartilhado (cache do Spring), então
 * cada teste começa limpando as tabelas.
 *
 * As propriedades abaixo são fixadas aqui porque o ./tests.sh carrega o .env e variáveis como AUTH_JWT_SECRET
 * (mesmo vazias) têm precedência sobre o application.yml de teste.
 */
@SpringBootTest(
    properties = [
        "auth.jwt.secret=" + TestTokens.SECRET,
        "aws.s3.region=us-east-1",
        "aws.s3.recordings.bucket=recordings-bucket-test",
    ]
)
@AutoConfigureMockMvc
abstract class IntegrationTest {

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    lateinit var mockMvc: MockMvc

    @BeforeEach
    fun cleanDatabase() {
        jdbcTemplate.execute("DELETE FROM transcription_utterance")
        jdbcTemplate.execute("DELETE FROM usage_record")
        jdbcTemplate.execute("DELETE FROM transcription")
    }
}
