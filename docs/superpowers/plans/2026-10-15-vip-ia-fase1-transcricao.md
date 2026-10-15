# vip-ia — Fase 1 (Transcrição) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Criar o serviço `vip-ia` (porta 8083) que consome as CDRs publicadas pelo manager, transcreve as gravações do S3 com a Deepgram, guarda tudo num MySQL próprio, registra o consumo e expõe as transcrições numa API REST protegida pelo JWT do manager.

**Architecture:** O listener Kafka só persiste tarefas (`transcription` em `PENDING`, uma por gravação). Um processador agendado reivindica as tarefas vencidas de forma atômica, baixa o `.mp3` do S3, chama a Deepgram e grava o resultado, as falas e o consumo numa única transação. Falhas viram `RETRY` com backoff exponencial ou `FAILED`. S3 e Deepgram ficam atrás das interfaces `RecordingStorage` e `TranscriptionProvider`, trocadas por fakes nos testes.

**Tech Stack:** Kotlin 2.3.21, JDK 21 (virtual threads), Spring Boot 4.1.0 (Jackson 3), Spring Data JDBC, Spring Kafka, Spring Security + `com.auth0:java-jwt:4.5.0`, AWS SDK v2 S3 (`bom:2.41.6`), Deepgram Java SDK `0.11.1`, MySQL 8 (produção), H2 modo MySQL (testes), JUnit 5 + MockK.

**Spec:** `vip-ia/MANIFESTO.md` (design) e `vip-ia/AGENTS.md` (convenções). Leia os dois antes de começar.

## Global Constraints

- Diretório do app: `/home/jefaokpta/Workspaces/vip-workspace/vip-ia`. Pacote base: `br.com.vipsolutions.vipia`.
- Versões **idênticas ao vip-worker**: Kotlin `2.3.21`, Spring Boot `4.1.0`, `io.spring.dependency-management` `1.1.7`, Gradle wrapper `8.14.3`, JDK toolchain `21`.
- Trabalhe **sempre na branch `dev`**. Nunca commite em `main`/`master`.
- Testes **sempre** via `./tests.sh` (exige `.env` na raiz do app); teste único via `./gradlew test --tests "<FQN>"`.
- Spring Data **JDBC** (nunca JPA). Entidades como `data class` com `val`; injeção **só por construtor** no código de produção (`@Autowired lateinit` só é aceito em classes de teste).
- Toda mudança de schema: novo arquivo em `src/main/resources/db/migration/` **e** reflexo manual em `src/test/resources/schema.sql` **e** entrada em `docs/db-migrations-log.md`. Nunca edite um `.sql` de migration já commitado.
- `companyId` vem **sempre** do JWT (`UserPrincipal.companyId`), nunca de parâmetro, exceto em `/internal/usage` (rota de `ROLE_SAAS_FINANCE`).
- JWT: `HMAC512(auth.jwt.secret)`, issuer `"VIP API"`, claims `companyId`, `tenantId`, `roles`, `managingCompany` (opcional).
- Chave S3 da gravação: `recordings/{companyId}/{recordName}.mp3`, bucket `aws.s3.recordings.bucket`.
- Deepgram: modelo `nova-3`, idioma `pt-BR`, `diarize`, `utterances`, `punctuate`, `smart_format` ligados; `maxRetries(0)` no cliente (o retry é nosso).
- Retry: backoff `retry-base-delay * 2^(attempts-1)`, `max-attempts` padrão `6`, `retry-base-delay` padrão `PT1M`.
- Comentários e logs em **pt-BR**. Nunca chame Deepgram/S3/Kafka reais em testes.
- Mensagens de commit em pt-BR no formato `tipo: descrição`, terminando com a linha `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (e a linha `Claude-Session` se a sessão a fornecer).

## Review Focus

1. **Gravação ainda não existe no S3** (o worker publica a CDR antes de subir o `.mp3`): a tarefa deve ir para `RETRY` com backoff e só virar `FAILED` depois de `max-attempts`. Teste: Task 8, `gravacao ausente vira RETRY com backoff` e `gravacao ausente na ultima tentativa vira FAILED`.
2. **A mesma CDR entregue duas vezes** (rebalance ou reenvio do Kafka): nenhuma linha duplicada e nenhum consumo duplicado. Teste: Task 4, `mensagem repetida nao duplica tarefas`.
3. **Usuário da empresa A pede uma transcrição da empresa B por id ou por uniqueId**: deve receber 404, sem vazar existência. Teste: Task 11, `transcricao de outra empresa retorna 404`.
4. **Mensagem Kafka malformada** (JSON inválido, campo obrigatório ausente, `companyId` em branco): é descartada com log e o consumo continua. Teste: Task 3, `json invalido retorna null` e `campo obrigatorio ausente retorna null`. Teste: Task 5, `payload malformado nao chama o servico`.
5. **Chamada sem fala** (silêncio, URA, caixa postal): a Deepgram devolve `utterances` vazio e transcrição `""`, e a tarefa deve virar `DONE` com texto vazio, sem erro. Teste: Task 7, `resposta sem fala gera resultado vazio`. Teste: Task 8, `transcricao vazia conclui como DONE`.

---

## Estrutura de arquivos

```
vip-ia/
├── AGENTS.md, MANIFESTO.md                       (já existem)
├── build.gradle.kts, settings.gradle.kts, gradlew, gradlew.bat, gradle/wrapper/*
├── build.sh, run.sh, tests.sh, deploy-production.sh, .env.example, .gitignore
├── systemd/vip-ia.service
├── docs/db-migrations-log.md
└── src/
    ├── main/kotlin/br/com/vipsolutions/vipia/
    │   ├── VipIaApplication.kt
    │   ├── config/ClockConfig.kt, SchedulingConfig.kt
    │   ├── common/PageResponse.kt
    │   ├── auth/JwtService.kt, UserPrincipal.kt, FilterToken.kt, SecurityConfig.kt
    │   ├── kafka/KafkaSecurityConfig.kt, KafkaConsumerConfig.kt, IaCdrListener.kt, IaCdrMessageParser.kt
    │   ├── kafka/message/IaCdrMessage.kt
    │   ├── recording/RecordingStorage.kt, RecordingNotFoundException.kt, S3RecordingStorage.kt, AwsS3Config.kt
    │   ├── deepgram/DeepgramProperties.kt, DeepgramConfig.kt, DeepgramTranscriptionProvider.kt, DeepgramResponseMapper.kt
    │   ├── transcription/TranscriptionStatus.kt, TranscriptionEntity.kt, TranscriptionRepository.kt
    │   ├── transcription/TranscriptionUtteranceEntity.kt, TranscriptionUtteranceRepository.kt
    │   ├── transcription/TranscriptionProperties.kt, TranscriptionTaskService.kt, RetryPolicy.kt
    │   ├── transcription/TranscriptionProcessor.kt, TranscriptionCompletionService.kt
    │   ├── transcription/TranscriptionScheduler.kt, StuckTranscriptionRecovery.kt
    │   ├── transcription/provider/TranscriptionProvider.kt   (interface + TranscriptionResult + Utterance + TranscriptionFailedException)
    │   ├── transcription/api/TranscriptionController.kt, TranscriptionApiService.kt, TranscriptionQueryRepository.kt
    │   ├── transcription/api/dto/TranscriptionDtos.kt
    │   ├── usage/UsageType.kt, UsageProvider.kt, UsageRecordEntity.kt, UsageRecordRepository.kt, UsageService.kt
    │   └── usage/api/UsageController.kt, UsageReportRepository.kt, dto/UsageDtos.kt
    ├── main/resources/application.yml
    ├── main/resources/db/migration/create_transcription.sql, create_transcription_utterance.sql, create_usage_record.sql
    ├── test/resources/application.yml, schema.sql
    ├── test/resources/kafka-contracts/ia-cdr-message.json
    ├── test/resources/deepgram/response-with-speech.json, response-silence.json
    └── test/kotlin/br/com/vipsolutions/vipia/
        ├── support/IntegrationTest.kt, TestTokens.kt, Fixtures.kt, FakeRecordingStorage.kt, FakeTranscriptionProvider.kt
        └── (um *Test.kt por unidade, no mesmo pacote da classe testada)
```

---

### Task 1: Bootstrap do projeto (Gradle, scripts, config, app sobe)

**Files:**
- Create: `vip-ia/settings.gradle.kts`, `vip-ia/build.gradle.kts`, `vip-ia/.gitignore`, `vip-ia/.env.example`
- Create (copiados do worker): `vip-ia/gradlew`, `vip-ia/gradlew.bat`, `vip-ia/gradle/wrapper/gradle-wrapper.jar`, `vip-ia/gradle/wrapper/gradle-wrapper.properties`
- Create: `vip-ia/build.sh`, `vip-ia/run.sh`, `vip-ia/tests.sh`, `vip-ia/deploy-production.sh`, `vip-ia/systemd/vip-ia.service`
- Create: `src/main/kotlin/br/com/vipsolutions/vipia/VipIaApplication.kt`, `config/ClockConfig.kt`
- Create: `src/main/resources/application.yml`, `src/test/resources/application.yml`
- Test: `src/test/kotlin/br/com/vipsolutions/vipia/actuator/ActuatorEndpointsTest.kt`

**Interfaces:**
- Produces: bean `java.time.Clock` (UTC), usado por todas as tasks seguintes. Propriedades `vip-ia.*` declaradas no `application.yml`.

- [ ] **Step 1: Inicializar o repositório git na branch `dev`**

```bash
cd /home/jefaokpta/Workspaces/vip-workspace/vip-ia
git init -b dev
cp ../vip-worker/gradlew ../vip-worker/gradlew.bat .
mkdir -p gradle/wrapper && cp ../vip-worker/gradle/wrapper/gradle-wrapper.jar ../vip-worker/gradle/wrapper/gradle-wrapper.properties gradle/wrapper/
cp ../vip-worker/.gitignore .
chmod +x gradlew
```

- [ ] **Step 2: Criar `settings.gradle.kts` e `build.gradle.kts`**

`settings.gradle.kts`:
```kotlin
rootProject.name = "vip-ia"
```

`build.gradle.kts` (as dependências de Kafka, S3, Deepgram e Security entram nas tasks que as usam):
```kotlin
plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    id("org.springframework.boot") version "4.1.0"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "br.com.vipsolutions"
version = "0.0.1-SNAPSHOT"
description = "vip-ia"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    runtimeOnly("com.mysql:mysql-connector-j")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("io.mockk:mockk:1.13.13")
    testRuntimeOnly("com.h2database:h2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
```

- [ ] **Step 3: Criar scripts e unit systemd**

`build.sh`, `run.sh` e `tests.sh` seguem o mesmo cabeçalho do vip-worker. Só muda a última linha: `sh gradlew clean build`, `sh gradlew bootRun` e `sh gradlew test`, respectivamente. Exemplo de `tests.sh`:
```sh
#!/usr/bin/env sh

# Export variables from .env if present
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
else
  echo "Warning: .env nao encontrado"
  exit 1
fi

sh gradlew test
```

`deploy-production.sh`:
```sh
#!/usr/bin/env sh

# Export variables from .env if present
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
else
  echo "Warning: .env nao encontrado"
  exit 1
fi

if [ -z "$DEPLOY_HOST" ]; then
  echo "❌ DEPLOY_HOST nao definido no .env"
  exit 1
fi

sh gradlew clean build

BUILD_EXIT_CODE=$?
if [ $BUILD_EXIT_CODE -ne 0 ]; then
    echo "❌ Build falhou"
    exit 1
fi

echo "🚚 Enviando jar para $DEPLOY_HOST"
scp build/libs/vip-ia-0.0.1-SNAPSHOT.jar root@$DEPLOY_HOST:/opt/vip-ia/vip-ia.jar

SCP_EXIT_CODE=$?
if [ $SCP_EXIT_CODE -ne 0 ]; then
    echo "❌ SCP falhou"
    exit 1
fi

echo "🚀 Reiniciando vip-ia"
ssh root@$DEPLOY_HOST "systemctl restart vip-ia.service"
```

`systemd/vip-ia.service`:
```ini
[Unit]
Description=VIP IA - TRANSCRICAO E AGENTES DE IA
After=network.target

[Service]
StandardOutput=journal
StandardError=journal
WorkingDirectory=/opt/vip-ia
## Usando SDKMAN o Java fica em /root/.sdkman/candidates/java/current/bin/java
ExecStart=/root/.sdkman/candidates/java/current/bin/java -jar vip-ia.jar
SuccessExitStatus=143

EnvironmentFile=/opt/vip-ia/.env

[Install]
WantedBy=multi-user.target
```

```bash
chmod +x build.sh run.sh tests.sh deploy-production.sh
```

- [ ] **Step 4: Criar `.env.example` e um `.env` local**

`.env.example`:
```dotenv
SERVER_PORT=8083
DB_URL=jdbc:mysql://localhost:3306/vip_ia
DB_USERNAME=vip_ia
DB_PASSWORD=
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=
KAFKA_GROUP_ID=vip-ia
KAFKA_MANAGER_IA_CDR_TOPIC=vip.manager.ia.cdr
AWS_S3_REGION=us-east-1
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_S3_RECORDINGS_BUCKET=
DEEPGRAM_API_KEY=
AUTH_JWT_SECRET=
DEPLOY_HOST=
```

```bash
cp .env.example .env   # .env já está no .gitignore copiado do worker
```

- [ ] **Step 5: Criar `application.yml` de produção**

`src/main/resources/application.yml`:
```yaml
server:
  port: ${SERVER_PORT:8083}
spring:
  application:
    name: vip-ia
  threads:
    virtual:
      enabled: true
  datasource:
    url: ${DB_URL}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
    driver-class-name: com.mysql.cj.jdbc.Driver
  sql:
    init:
      mode: never
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
    sasl:
      username: ${KAFKA_SASL_USERNAME}
      password: ${KAFKA_SASL_PASSWORD}
    consumer:
      group-id: ${KAFKA_GROUP_ID}
      client-id: vip-ia-consumer
      ia-cdr-topic: ${KAFKA_MANAGER_IA_CDR_TOPIC}

aws:
  s3:
    region: ${AWS_S3_REGION}
    recordings:
      bucket: ${AWS_S3_RECORDINGS_BUCKET}

deepgram:
  api-key: ${DEEPGRAM_API_KEY}
  timeout-seconds: 300

auth:
  jwt:
    secret: ${AUTH_JWT_SECRET}

vip-ia:
  kafka:
    listener:
      auto-startup: true
  scheduling:
    enabled: true
  transcription:
    poll-interval: PT10S
    batch-size: 20
    max-concurrency: 4
    max-attempts: 6
    retry-base-delay: PT1M
    language: pt-BR

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus
      discovery:
        enabled: false
  endpoint:
    health:
      show-details: never
```

- [ ] **Step 6: Criar `application.yml` de teste** (substitui o de produção no classpath de teste)

`src/test/resources/application.yml`:
```yaml
server:
  port: 8083
spring:
  application:
    name: vip-ia-test
  threads:
    virtual:
      enabled: true
  datasource:
    url: jdbc:h2:mem:vipia;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE
    username: sa
    password:
    driver-class-name: org.h2.Driver
  sql:
    init:
      mode: always
  kafka:
    bootstrap-servers: localhost:9092
    sasl:
      username: test-user
      password: test-password
    consumer:
      group-id: test-group
      client-id: test-client
      ia-cdr-topic: test-ia-cdr-topic

aws:
  s3:
    region: us-east-1
    recordings:
      bucket: recordings-bucket-test

deepgram:
  api-key: test-deepgram-key
  timeout-seconds: 30

auth:
  jwt:
    secret: test-secret

vip-ia:
  kafka:
    listener:
      auto-startup: false
  scheduling:
    enabled: false
  transcription:
    poll-interval: PT10S
    batch-size: 20
    max-concurrency: 4
    max-attempts: 6
    retry-base-delay: PT1M
    language: pt-BR

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus
      discovery:
        enabled: false
  endpoint:
    health:
      show-details: never
```

- [ ] **Step 7: Escrever o teste que falha**

`src/test/kotlin/br/com/vipsolutions/vipia/actuator/ActuatorEndpointsTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.actuator

import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@SpringBootTest
@AutoConfigureMockMvc
class ActuatorEndpointsTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Test
    fun `GET actuator health retorna 200 sem autenticacao`() {
        mockMvc.perform(get("/actuator/health"))
            .andExpect(status().isOk)
            .andExpect(content().json("""{"status":"UP"}"""))
    }

    @Test
    fun `GET actuator prometheus retorna metricas sem autenticacao`() {
        mockMvc.perform(get("/actuator/prometheus"))
            .andExpect(status().isOk)
            .andExpect(content().string(containsString("jvm_memory_used_bytes")))
    }
}
```

- [ ] **Step 8: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.actuator.ActuatorEndpointsTest"`
Expected: FAIL. Não há classe `@SpringBootApplication` ("Unable to find a @SpringBootConfiguration").

- [ ] **Step 9: Criar a aplicação e o `Clock`**

`src/main/kotlin/br/com/vipsolutions/vipia/VipIaApplication.kt`:
```kotlin
package br.com.vipsolutions.vipia

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class VipIaApplication

fun main(args: Array<String>) {
    runApplication<VipIaApplication>(*args)
}
```

`src/main/kotlin/br/com/vipsolutions/vipia/config/ClockConfig.kt`:
```kotlin
package br.com.vipsolutions.vipia.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

/**
 * Relógio único da aplicação: injetado em tudo que calcula horários (backoff, created_at) para os testes
 * poderem usar um Clock fixo.
 */
@Configuration
class ClockConfig {

    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
```

- [ ] **Step 10: Rodar e ver passar**

Run: `./tests.sh`
Expected: PASS (2 testes).

- [ ] **Step 11: Commit** (inclui os documentos já existentes)

```bash
git add .
git commit -m "chore: bootstrap do vip-ia (gradle, scripts, configuracao, actuator)"
```

---

### Task 2: Schema, entidades e repositórios

**Files:**
- Create: `src/main/resources/db/migration/create_transcription.sql`, `create_transcription_utterance.sql`, `create_usage_record.sql`
- Create: `src/test/resources/schema.sql`, `docs/db-migrations-log.md`
- Create: `transcription/TranscriptionStatus.kt`, `TranscriptionEntity.kt`, `TranscriptionRepository.kt`, `TranscriptionUtteranceEntity.kt`, `TranscriptionUtteranceRepository.kt`
- Create: `usage/UsageType.kt`, `UsageProvider.kt`, `UsageRecordEntity.kt`, `UsageRecordRepository.kt`
- Test: `support/IntegrationTest.kt`, `support/Fixtures.kt`, `transcription/TranscriptionRepositoryTest.kt`

**Interfaces:**
- Produces (usado nas Tasks 4, 8, 9, 11 e 12):
  - `enum class TranscriptionStatus { PENDING, PROCESSING, RETRY, DONE, FAILED }`
  - `TranscriptionEntity(id: Long?, companyId, cdrUniqueId, recordName, status, attempts: Int, nextAttemptAt: Instant, lastError: String?, callStartTime: Instant, src, destination, peer, disposition, userfield, callDuration: Int, audioSeconds: Double?, language: String?, model: String?, deepgramRequestId: String?, transcript: String?, createdAt: Instant, updatedAt: Instant)`
  - `TranscriptionRepository`: `existsByCompanyIdAndCdrUniqueIdAndRecordName(String, String, String): Boolean`, `findByIdAndCompanyId(Long, String): TranscriptionEntity?`, `findByCompanyIdAndCdrUniqueIdOrderByRecordName(String, String): List<TranscriptionEntity>`, `findDue(now: Instant, limit: Int): List<TranscriptionEntity>`, `claim(id: Long, now: Instant): Int`, `markRetry(id: Long, attempts: Int, nextAttemptAt: Instant, error: String, now: Instant): Int`, `markFailed(id: Long, attempts: Int, error: String, now: Instant): Int`, `markDone(id: Long, audioSeconds: Double, language: String, model: String, requestId: String, transcript: String, now: Instant): Int`, `resetStuck(now: Instant): Int`, `resetFailedForRetry(id: Long, companyId: String, now: Instant): Int`
  - `TranscriptionUtteranceEntity(id: Long?, transcriptionId: Long, speaker: Int, startSeconds: Double, endSeconds: Double, content: String, confidence: Double?)`
  - `TranscriptionUtteranceRepository`: `findByTranscriptionIdOrderByStartSeconds(Long)`, `deleteByTranscriptionId(Long): Int`
  - `enum class UsageType { TRANSCRIPTION, AGENT_RUN }`, `enum class UsageProvider { DEEPGRAM, OPENROUTER }`
  - `UsageRecordEntity(id: Long?, companyId, usageType: UsageType, referenceId: Long, provider: UsageProvider, model: String?, audioSeconds: Double?, inputTokens: Int?, outputTokens: Int?, cost: BigDecimal?, createdAt: Instant)`
  - `UsageRecordRepository : CrudRepository<UsageRecordEntity, Long>`, com `findByReferenceIdAndUsageType(Long, UsageType): List<UsageRecordEntity>`
  - Teste: `abstract class IntegrationTest` (limpa as tabelas antes de cada teste e expõe `mockMvc` e `jdbcTemplate`); `Fixtures.task(...)`.

- [ ] **Step 1: Migrations de produção (MySQL)**

`src/main/resources/db/migration/create_transcription.sql`:
```sql
CREATE TABLE transcription
(
    id                  BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    company_id          VARCHAR(255)  NOT NULL,
    cdr_unique_id       VARCHAR(255)  NOT NULL,
    record_name         VARCHAR(255)  NOT NULL,
    status              VARCHAR(20)   NOT NULL,
    attempts            INT           NOT NULL DEFAULT 0,
    next_attempt_at     DATETIME(3)   NOT NULL,
    last_error          VARCHAR(1000) NULL,
    call_start_time     DATETIME(3)   NOT NULL,
    src                 VARCHAR(255)  NOT NULL,
    destination         VARCHAR(255)  NOT NULL,
    peer                VARCHAR(255)  NOT NULL,
    disposition         VARCHAR(50)   NOT NULL,
    userfield           VARCHAR(255)  NOT NULL,
    call_duration       INT           NOT NULL,
    audio_seconds       DOUBLE        NULL,
    language            VARCHAR(20)   NULL,
    model               VARCHAR(50)   NULL,
    deepgram_request_id VARCHAR(255)  NULL,
    transcript          MEDIUMTEXT    NULL,
    created_at          DATETIME(3)   NOT NULL,
    updated_at          DATETIME(3)   NOT NULL,
    UNIQUE KEY uk_transcription_cdr_record (company_id, cdr_unique_id, record_name),
    INDEX idx_transcription_company_start (company_id, call_start_time),
    INDEX idx_transcription_due (status, next_attempt_at)
);
```

`src/main/resources/db/migration/create_transcription_utterance.sql`:
```sql
CREATE TABLE transcription_utterance
(
    id               BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    transcription_id BIGINT UNSIGNED NOT NULL,
    speaker          INT             NOT NULL,
    start_seconds    DOUBLE          NOT NULL,
    end_seconds      DOUBLE          NOT NULL,
    content          TEXT            NOT NULL,
    confidence       DOUBLE          NULL,
    INDEX idx_utterance_transcription (transcription_id, start_seconds),
    CONSTRAINT fk_utterance_transcription FOREIGN KEY (transcription_id) REFERENCES transcription (id) ON DELETE CASCADE
);
```

`src/main/resources/db/migration/create_usage_record.sql`:
```sql
CREATE TABLE usage_record
(
    id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    company_id    VARCHAR(255)    NOT NULL,
    usage_type    VARCHAR(20)     NOT NULL,
    reference_id  BIGINT UNSIGNED NOT NULL,
    provider      VARCHAR(20)     NOT NULL,
    model         VARCHAR(100)    NULL,
    audio_seconds DOUBLE          NULL,
    input_tokens  INT             NULL,
    output_tokens INT             NULL,
    cost          DECIMAL(15, 6)  NULL,
    created_at    DATETIME(3)     NOT NULL,
    INDEX idx_usage_company_created (company_id, created_at),
    INDEX idx_usage_reference (reference_id, usage_type)
);
```

- [ ] **Step 2: Schema H2 de teste (mantido à mão, espelho das migrations)**

`src/test/resources/schema.sql`:
```sql
-- Espelho manual de src/main/resources/db/migration/*.sql para o H2 (modo MySQL).
-- Toda migration nova precisa ser refletida aqui.
CREATE TABLE IF NOT EXISTS transcription
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    company_id          VARCHAR(255)  NOT NULL,
    cdr_unique_id       VARCHAR(255)  NOT NULL,
    record_name         VARCHAR(255)  NOT NULL,
    status              VARCHAR(20)   NOT NULL,
    attempts            INT           NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMP(3)  NOT NULL,
    last_error          VARCHAR(1000),
    call_start_time     TIMESTAMP(3)  NOT NULL,
    src                 VARCHAR(255)  NOT NULL,
    destination         VARCHAR(255)  NOT NULL,
    peer                VARCHAR(255)  NOT NULL,
    disposition         VARCHAR(50)   NOT NULL,
    userfield           VARCHAR(255)  NOT NULL,
    call_duration       INT           NOT NULL,
    audio_seconds       DOUBLE,
    language            VARCHAR(20),
    model               VARCHAR(50),
    deepgram_request_id VARCHAR(255),
    transcript          TEXT,
    created_at          TIMESTAMP(3)  NOT NULL,
    updated_at          TIMESTAMP(3)  NOT NULL,
    CONSTRAINT uk_transcription_cdr_record UNIQUE (company_id, cdr_unique_id, record_name)
);

CREATE TABLE IF NOT EXISTS transcription_utterance
(
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    transcription_id BIGINT NOT NULL,
    speaker          INT    NOT NULL,
    start_seconds    DOUBLE NOT NULL,
    end_seconds      DOUBLE NOT NULL,
    content          TEXT   NOT NULL,
    confidence       DOUBLE,
    CONSTRAINT fk_utterance_transcription FOREIGN KEY (transcription_id) REFERENCES transcription (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS usage_record
(
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    company_id    VARCHAR(255)   NOT NULL,
    usage_type    VARCHAR(20)    NOT NULL,
    reference_id  BIGINT         NOT NULL,
    provider      VARCHAR(20)    NOT NULL,
    model         VARCHAR(100),
    audio_seconds DOUBLE,
    input_tokens  INT,
    output_tokens INT,
    cost          DECIMAL(15, 6),
    created_at    TIMESTAMP(3)   NOT NULL
);
```

- [ ] **Step 3: Log de migrations**

`docs/db-migrations-log.md`:
```markdown
# Log de migrations — vip-ia

Scripts a rodar manualmente no MySQL de produção do vip-ia, na ordem.

## 2026-10-15 — Fase 1 (transcrição)

Pré-requisito: `CREATE DATABASE vip_ia CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;`

1. `src/main/resources/db/migration/create_transcription.sql`
2. `src/main/resources/db/migration/create_transcription_utterance.sql`
3. `src/main/resources/db/migration/create_usage_record.sql`
```

- [ ] **Step 4: Base de testes de integração e fixtures**

`src/test/kotlin/br/com/vipsolutions/vipia/support/IntegrationTest.kt`:
```kotlin
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
 */
@SpringBootTest
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
```

`src/test/kotlin/br/com/vipsolutions/vipia/support/Fixtures.kt`:
```kotlin
package br.com.vipsolutions.vipia.support

import br.com.vipsolutions.vipia.transcription.TranscriptionEntity
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import java.time.Instant

object Fixtures {

    val NOW: Instant = Instant.parse("2026-10-15T12:00:00Z")

    fun task(
        companyId: String = "100023",
        cdrUniqueId: String = "1772633126.1880",
        recordName: String = "100023-1772633126.1880-MIXED",
        status: TranscriptionStatus = TranscriptionStatus.PENDING,
        attempts: Int = 0,
        nextAttemptAt: Instant = NOW.minusSeconds(1),
        callStartTime: Instant = Instant.parse("2026-10-14T15:30:00Z"),
        src: String = "12_100023",
        destination: String = "1132931515",
        peer: String = "12",
    ) = TranscriptionEntity(
        companyId = companyId,
        cdrUniqueId = cdrUniqueId,
        recordName = recordName,
        status = status,
        attempts = attempts,
        nextAttemptAt = nextAttemptAt,
        callStartTime = callStartTime,
        src = src,
        destination = destination,
        peer = peer,
        disposition = "ANSWERED",
        userfield = "OUTBOUND",
        callDuration = 42,
        createdAt = NOW,
        updatedAt = NOW,
    )
}
```

- [ ] **Step 5: Escrever o teste que falha**

`src/test/kotlin/br/com/vipsolutions/vipia/transcription/TranscriptionRepositoryTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.support.Fixtures
import br.com.vipsolutions.vipia.support.Fixtures.NOW
import br.com.vipsolutions.vipia.support.IntegrationTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.relational.core.conversion.DbActionExecutionException

class TranscriptionRepositoryTest : IntegrationTest() {

    @Autowired
    lateinit var repository: TranscriptionRepository

    @Autowired
    lateinit var utteranceRepository: TranscriptionUtteranceRepository

    @Test
    fun `salva e le uma tarefa com todos os campos`() {
        val saved = repository.save(Fixtures.task())

        val loaded = repository.findByIdAndCompanyId(saved.id!!, "100023")!!
        assertEquals(TranscriptionStatus.PENDING, loaded.status)
        assertEquals(Fixtures.task().callStartTime, loaded.callStartTime)
        assertEquals("100023-1772633126.1880-MIXED", loaded.recordName)
    }

    @Test
    fun `findByIdAndCompanyId nao retorna tarefa de outra empresa`() {
        val saved = repository.save(Fixtures.task(companyId = "100023"))

        assertNull(repository.findByIdAndCompanyId(saved.id!!, "999999"))
    }

    @Test
    fun `chave unica impede a mesma gravacao duas vezes`() {
        repository.save(Fixtures.task())

        assertThrows<DbActionExecutionException> { repository.save(Fixtures.task()) }
    }

    @Test
    fun `findDue traz so PENDING e RETRY vencidas, em ordem e com limite`() {
        repository.save(Fixtures.task(recordName = "a", nextAttemptAt = NOW.minusSeconds(30)))
        repository.save(Fixtures.task(recordName = "b", status = TranscriptionStatus.RETRY, nextAttemptAt = NOW.minusSeconds(60)))
        repository.save(Fixtures.task(recordName = "c", nextAttemptAt = NOW.plusSeconds(60)))
        repository.save(Fixtures.task(recordName = "d", status = TranscriptionStatus.DONE))
        repository.save(Fixtures.task(recordName = "e", status = TranscriptionStatus.PROCESSING))

        val due = repository.findDue(NOW, 10)
        assertEquals(listOf("b", "a"), due.map { it.recordName })
        assertEquals(1, repository.findDue(NOW, 1).size)
    }

    @Test
    fun `claim so funciona uma vez`() {
        val id = repository.save(Fixtures.task()).id!!

        assertEquals(1, repository.claim(id, NOW))
        assertEquals(0, repository.claim(id, NOW))
        assertEquals(TranscriptionStatus.PROCESSING, repository.findById(id).get().status)
    }

    @Test
    fun `markRetry, markFailed e markDone atualizam estado`() {
        val id = repository.save(Fixtures.task()).id!!

        repository.markRetry(id, 1, NOW.plusSeconds(60), "sem audio", NOW)
        repository.findById(id).get().let {
            assertEquals(TranscriptionStatus.RETRY, it.status)
            assertEquals(1, it.attempts)
            assertEquals(NOW.plusSeconds(60), it.nextAttemptAt)
            assertEquals("sem audio", it.lastError)
        }

        repository.markFailed(id, 2, "erro fatal", NOW)
        assertEquals(TranscriptionStatus.FAILED, repository.findById(id).get().status)

        repository.markDone(id, 42.5, "pt-BR", "nova-3", "req-1", "Olá", NOW)
        repository.findById(id).get().let {
            assertEquals(TranscriptionStatus.DONE, it.status)
            assertEquals(42.5, it.audioSeconds)
            assertEquals("Olá", it.transcript)
            assertNull(it.lastError)
        }
    }

    @Test
    fun `resetStuck devolve PROCESSING para RETRY`() {
        val id = repository.save(Fixtures.task(status = TranscriptionStatus.PROCESSING)).id!!

        assertEquals(1, repository.resetStuck(NOW))
        repository.findById(id).get().let {
            assertEquals(TranscriptionStatus.RETRY, it.status)
            assertEquals(NOW, it.nextAttemptAt)
        }
    }

    @Test
    fun `resetFailedForRetry so reabre FAILED da propria empresa`() {
        val failed = repository.save(Fixtures.task(status = TranscriptionStatus.FAILED, attempts = 6)).id!!
        val done = repository.save(Fixtures.task(recordName = "x", status = TranscriptionStatus.DONE)).id!!

        assertEquals(0, repository.resetFailedForRetry(failed, "999999", NOW))
        assertEquals(0, repository.resetFailedForRetry(done, "100023", NOW))
        assertEquals(1, repository.resetFailedForRetry(failed, "100023", NOW))
        repository.findById(failed).get().let {
            assertEquals(TranscriptionStatus.PENDING, it.status)
            assertEquals(0, it.attempts)
        }
    }

    @Test
    fun `utterances sao salvas, lidas em ordem e apagadas por transcricao`() {
        val id = repository.save(Fixtures.task()).id!!
        utteranceRepository.saveAll(
            listOf(
                TranscriptionUtteranceEntity(transcriptionId = id, speaker = 1, startSeconds = 2.0, endSeconds = 3.0, content = "segunda", confidence = 0.9),
                TranscriptionUtteranceEntity(transcriptionId = id, speaker = 0, startSeconds = 0.5, endSeconds = 1.5, content = "primeira", confidence = null),
            )
        )

        assertEquals(listOf("primeira", "segunda"), utteranceRepository.findByTranscriptionIdOrderByStartSeconds(id).map { it.content })
        assertEquals(2, utteranceRepository.deleteByTranscriptionId(id))
        assertTrue(utteranceRepository.findByTranscriptionIdOrderByStartSeconds(id).isEmpty())
    }
}
```

- [ ] **Step 6: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.TranscriptionRepositoryTest"`
Expected: FAIL de compilação (`TranscriptionRepository`, `TranscriptionEntity` etc. não existem).

- [ ] **Step 7: Implementar enums, entidades e repositórios**

`transcription/TranscriptionStatus.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

enum class TranscriptionStatus { PENDING, PROCESSING, RETRY, DONE, FAILED }
```

`transcription/TranscriptionEntity.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant

/**
 * Uma tarefa de transcrição = uma gravação de uma CDR. Também guarda o resultado (texto, duração, modelo).
 * Mudanças de estado são feitas por UPDATEs explícitos no repositório (ver TranscriptionRepository), nunca
 * por save() da entidade inteira, para não sobrescrever campos em corrida.
 */
@Table("transcription")
data class TranscriptionEntity(
    @Id
    val id: Long? = null,
    val companyId: String,
    val cdrUniqueId: String,
    val recordName: String,
    val status: TranscriptionStatus,
    val attempts: Int = 0,
    val nextAttemptAt: Instant,
    val lastError: String? = null,
    val callStartTime: Instant,
    val src: String,
    val destination: String,
    val peer: String,
    val disposition: String,
    val userfield: String,
    val callDuration: Int,
    val audioSeconds: Double? = null,
    val language: String? = null,
    val model: String? = null,
    val deepgramRequestId: String? = null,
    val transcript: String? = null,
    val createdAt: Instant,
    val updatedAt: Instant,
)
```

`transcription/TranscriptionRepository.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.springframework.data.jdbc.repository.query.Modifying
import org.springframework.data.jdbc.repository.query.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param
import java.time.Instant

interface TranscriptionRepository : CrudRepository<TranscriptionEntity, Long> {

    fun existsByCompanyIdAndCdrUniqueIdAndRecordName(companyId: String, cdrUniqueId: String, recordName: String): Boolean

    fun findByIdAndCompanyId(id: Long, companyId: String): TranscriptionEntity?

    fun findByCompanyIdAndCdrUniqueIdOrderByRecordName(companyId: String, cdrUniqueId: String): List<TranscriptionEntity>

    @Query(
        """SELECT * FROM transcription
           WHERE status IN ('PENDING', 'RETRY') AND next_attempt_at <= :now
           ORDER BY next_attempt_at
           LIMIT :limit"""
    )
    fun findDue(@Param("now") now: Instant, @Param("limit") limit: Int): List<TranscriptionEntity>

    /** Reivindica a tarefa de forma atômica: retorna 1 só para quem conseguiu mudar para PROCESSING. */
    @Modifying
    @Query("UPDATE transcription SET status = 'PROCESSING', updated_at = :now WHERE id = :id AND status IN ('PENDING', 'RETRY')")
    fun claim(@Param("id") id: Long, @Param("now") now: Instant): Int

    @Modifying
    @Query(
        """UPDATE transcription
           SET status = 'RETRY', attempts = :attempts, next_attempt_at = :nextAttemptAt, last_error = :error, updated_at = :now
           WHERE id = :id"""
    )
    fun markRetry(
        @Param("id") id: Long,
        @Param("attempts") attempts: Int,
        @Param("nextAttemptAt") nextAttemptAt: Instant,
        @Param("error") error: String,
        @Param("now") now: Instant,
    ): Int

    @Modifying
    @Query("UPDATE transcription SET status = 'FAILED', attempts = :attempts, last_error = :error, updated_at = :now WHERE id = :id")
    fun markFailed(
        @Param("id") id: Long,
        @Param("attempts") attempts: Int,
        @Param("error") error: String,
        @Param("now") now: Instant,
    ): Int

    @Modifying
    @Query(
        """UPDATE transcription
           SET status = 'DONE', audio_seconds = :audioSeconds, language = :language, model = :model,
               deepgram_request_id = :requestId, transcript = :transcript, last_error = NULL, updated_at = :now
           WHERE id = :id"""
    )
    fun markDone(
        @Param("id") id: Long,
        @Param("audioSeconds") audioSeconds: Double,
        @Param("language") language: String,
        @Param("model") model: String,
        @Param("requestId") requestId: String,
        @Param("transcript") transcript: String,
        @Param("now") now: Instant,
    ): Int

    /** Usado no boot: tarefas que ficaram em PROCESSING (app caiu no meio) voltam para a fila. */
    @Modifying
    @Query("UPDATE transcription SET status = 'RETRY', next_attempt_at = :now, updated_at = :now WHERE status = 'PROCESSING'")
    fun resetStuck(@Param("now") now: Instant): Int

    @Modifying
    @Query(
        """UPDATE transcription
           SET status = 'PENDING', attempts = 0, next_attempt_at = :now, last_error = NULL, updated_at = :now
           WHERE id = :id AND company_id = :companyId AND status = 'FAILED'"""
    )
    fun resetFailedForRetry(@Param("id") id: Long, @Param("companyId") companyId: String, @Param("now") now: Instant): Int
}
```

`transcription/TranscriptionUtteranceEntity.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

/** Uma fala contínua de um locutor (diarização da Deepgram). */
@Table("transcription_utterance")
data class TranscriptionUtteranceEntity(
    @Id
    val id: Long? = null,
    val transcriptionId: Long,
    val speaker: Int,
    val startSeconds: Double,
    val endSeconds: Double,
    val content: String,
    val confidence: Double?,
)
```

`transcription/TranscriptionUtteranceRepository.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.springframework.data.jdbc.repository.query.Modifying
import org.springframework.data.jdbc.repository.query.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param

interface TranscriptionUtteranceRepository : CrudRepository<TranscriptionUtteranceEntity, Long> {

    fun findByTranscriptionIdOrderByStartSeconds(transcriptionId: Long): List<TranscriptionUtteranceEntity>

    @Modifying
    @Query("DELETE FROM transcription_utterance WHERE transcription_id = :transcriptionId")
    fun deleteByTranscriptionId(@Param("transcriptionId") transcriptionId: Long): Int
}
```

`usage/UsageType.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage

enum class UsageType { TRANSCRIPTION, AGENT_RUN }
```

`usage/UsageProvider.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage

enum class UsageProvider { DEEPGRAM, OPENROUTER }
```

`usage/UsageRecordEntity.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

/**
 * Consumo de um provedor externo. A Deepgram não informa custo na resposta: em TRANSCRIPTION o cost fica
 * NULL e o relatório financeiro aplica o preço por minuto sobre audioSeconds.
 */
@Table("usage_record")
data class UsageRecordEntity(
    @Id
    val id: Long? = null,
    val companyId: String,
    val usageType: UsageType,
    val referenceId: Long,
    val provider: UsageProvider,
    val model: String?,
    val audioSeconds: Double? = null,
    val inputTokens: Int? = null,
    val outputTokens: Int? = null,
    val cost: BigDecimal? = null,
    val createdAt: Instant,
)
```

`usage/UsageRecordRepository.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage

import org.springframework.data.repository.CrudRepository

interface UsageRecordRepository : CrudRepository<UsageRecordEntity, Long> {

    fun findByReferenceIdAndUsageType(referenceId: Long, usageType: UsageType): List<UsageRecordEntity>
}
```

- [ ] **Step 8: Rodar e ver passar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.TranscriptionRepositoryTest"`
Expected: PASS (9 testes).

Se `Instant` não fizer a ida e volta corretamente no H2 (diferença de fuso), não troque para `LocalDateTime`. Confira se o schema usa `TIMESTAMP(3)` e se o `Clock` é UTC, e aplique `superpowers:systematic-debugging`.

Se o Spring Data JDBC 4 lançar outra exceção no insert duplicado em vez de `DbActionExecutionException`, use no teste a exceção real e ajuste também o `catch` do `TranscriptionTaskService` (Task 4). Hoje o cenário esperado é `DbActionExecutionException` com causa `DuplicateKeyException`.

- [ ] **Step 9: Rodar a suíte inteira**

Run: `./tests.sh`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add .
git commit -m "feat: schema, entidades e repositorios de transcricao e consumo"
```

---

### Task 3: Mensagem Kafka do manager, parser e contrato

**Files:**
- Create: `kafka/message/IaCdrMessage.kt`, `kafka/IaCdrMessageParser.kt`
- Create: `src/test/resources/kafka-contracts/ia-cdr-message.json`
- Test: `kafka/IaCdrMessageParserTest.kt`

**Interfaces:**
- Produces:
  - `data class IaCdrMessage(companyId: String, uniqueId: String, startTime: Long /*epoch millis*/, duration: Int, billableSeconds: Int, src: String, destination: String, peer: String, disposition: String, userfield: String, recordNames: List<String> = emptyList())`
  - `IaCdrMessageParser(objectMapper: tools.jackson.databind.ObjectMapper)` com `fun parse(payload: String): IaCdrMessage?`, que devolve `null` (e loga) para payload inválido.

- [ ] **Step 1: Golden file do contrato**

`src/test/resources/kafka-contracts/ia-cdr-message.json`:
```json
{
  "companyId": "100023",
  "uniqueId": "1772633126.1880",
  "startTime": 1772633126000,
  "duration": 8,
  "billableSeconds": 2,
  "src": "12_100023",
  "destination": "1132931515",
  "peer": "12",
  "disposition": "ANSWERED",
  "userfield": "OUTBOUND",
  "recordNames": ["100023-1772633126.1880-MIXED"]
}
```

- [ ] **Step 2: Escrever o teste que falha**

`src/test/kotlin/br/com/vipsolutions/vipia/kafka/IaCdrMessageParserTest.kt`:
```kotlin
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
```

- [ ] **Step 3: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.kafka.IaCdrMessageParserTest"`
Expected: FAIL de compilação (`IaCdrMessageParser` não existe).

- [ ] **Step 4: Implementar**

`kafka/message/IaCdrMessage.kt`:
```kotlin
package br.com.vipsolutions.vipia.kafka.message

/**
 * CDR republicada pelo vip-pabx-manager no tópico KAFKA_MANAGER_IA_CDR_TOPIC, só para empresas com o módulo de IA.
 * Contrato: src/test/resources/kafka-contracts/ia-cdr-message.json (espelhado no manager).
 *
 * recordNames são os mixedRecord (sem extensão) das channelInteractions da CDR — o callRecords da CDR do
 * worker vem sempre vazio e não é usado.
 */
data class IaCdrMessage(
    val companyId: String,
    val uniqueId: String,
    val startTime: Long,
    val duration: Int,
    val billableSeconds: Int,
    val src: String,
    val destination: String,
    val peer: String,
    val disposition: String,
    val userfield: String,
    val recordNames: List<String> = emptyList(),
)
```

`kafka/IaCdrMessageParser.kt`:
```kotlin
package br.com.vipsolutions.vipia.kafka

import br.com.vipsolutions.vipia.kafka.message.IaCdrMessage
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import tools.jackson.core.JacksonException
import tools.jackson.databind.ObjectMapper

/**
 * Converte o payload cru do Kafka. Mensagem inválida é logada e descartada (retorna null) para não travar
 * o consumo da partição.
 */
@Component
class IaCdrMessageParser(private val objectMapper: ObjectMapper) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun parse(payload: String): IaCdrMessage? {
        val message = try {
            objectMapper.readValue(payload, IaCdrMessage::class.java)
        } catch (e: JacksonException) {
            logger.warn("Mensagem de CDR invalida descartada: ${e.originalMessage} | payload=${payload.take(500)}")
            return null
        }
        if (message.companyId.isBlank() || message.uniqueId.isBlank()) {
            logger.warn("Mensagem de CDR sem companyId/uniqueId descartada | payload=${payload.take(500)}")
            return null
        }
        return message
    }
}
```

- [ ] **Step 5: Rodar e ver passar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.kafka.IaCdrMessageParserTest"`
Expected: PASS (6 testes).

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "feat: mensagem de CDR do manager, parser tolerante e contrato"
```

---

### Task 4: Enfileirar tarefas a partir da CDR (idempotente)

**Files:**
- Create: `transcription/TranscriptionTaskService.kt`
- Test: `transcription/TranscriptionTaskServiceTest.kt`

**Interfaces:**
- Consumes: `IaCdrMessage` (Task 3), `TranscriptionRepository`, `TranscriptionEntity` e `TranscriptionStatus` (Task 2), `Clock` (Task 1).
- Produces: `TranscriptionTaskService.enqueue(message: IaCdrMessage): Int`, que devolve quantas tarefas novas foram criadas.

- [ ] **Step 1: Escrever o teste que falha**

`src/test/kotlin/br/com/vipsolutions/vipia/transcription/TranscriptionTaskServiceTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.kafka.message.IaCdrMessage
import br.com.vipsolutions.vipia.support.IntegrationTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Instant

class TranscriptionTaskServiceTest : IntegrationTest() {

    @Autowired
    lateinit var service: TranscriptionTaskService

    @Autowired
    lateinit var repository: TranscriptionRepository

    private fun message(recordNames: List<String>) = IaCdrMessage(
        companyId = "100023",
        uniqueId = "1772633126.1880",
        startTime = 1772633126000,
        duration = 8,
        billableSeconds = 2,
        src = "12_100023",
        destination = "1132931515",
        peer = "12",
        disposition = "ANSWERED",
        userfield = "OUTBOUND",
        recordNames = recordNames,
    )

    @Test
    fun `cria uma tarefa PENDING por gravacao com os metadados da CDR`() {
        val created = service.enqueue(message(listOf("rec-1", "rec-2")))

        assertEquals(2, created)
        val tasks = repository.findByCompanyIdAndCdrUniqueIdOrderByRecordName("100023", "1772633126.1880")
        assertEquals(listOf("rec-1", "rec-2"), tasks.map { it.recordName })
        tasks.forEach {
            assertEquals(TranscriptionStatus.PENDING, it.status)
            assertEquals(0, it.attempts)
            assertEquals(Instant.ofEpochMilli(1772633126000), it.callStartTime)
            assertEquals("12_100023", it.src)
            assertEquals("1132931515", it.destination)
            assertEquals("12", it.peer)
            assertEquals(8, it.callDuration)
        }
    }

    @Test
    fun `mensagem repetida nao duplica tarefas`() {
        service.enqueue(message(listOf("rec-1")))
        val createdAgain = service.enqueue(message(listOf("rec-1", "rec-2")))

        assertEquals(1, createdAgain)
        assertEquals(2, repository.count())
    }

    @Test
    fun `nomes repetidos ou em branco na mesma mensagem sao ignorados`() {
        assertEquals(1, service.enqueue(message(listOf("rec-1", "rec-1", " ", ""))))
    }

    @Test
    fun `CDR sem gravacao nao cria tarefa`() {
        assertEquals(0, service.enqueue(message(emptyList())))
        assertEquals(0, repository.count())
    }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.TranscriptionTaskServiceTest"`
Expected: FAIL de compilação (`TranscriptionTaskService` não existe).

- [ ] **Step 3: Implementar**

`transcription/TranscriptionTaskService.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.kafka.message.IaCdrMessage
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.relational.core.conversion.DbActionExecutionException
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Instant

/**
 * Transforma uma CDR em tarefas de transcrição (uma por gravação). Idempotente: a chave única
 * (company_id, cdr_unique_id, record_name) garante que reentregas do Kafka não dupliquem trabalho nem custo.
 */
@Service
class TranscriptionTaskService(
    private val repository: TranscriptionRepository,
    private val clock: Clock,
) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun enqueue(message: IaCdrMessage): Int {
        val recordNames = message.recordNames.map(String::trim).filter(String::isNotEmpty).distinct()
        if (recordNames.isEmpty()) {
            logger.info("${message.uniqueId} >> CDR sem gravacao, nada a transcrever (empresa ${message.companyId})")
            return 0
        }
        val now = clock.instant()
        return recordNames.count { recordName -> createTask(message, recordName, now) }
    }

    private fun createTask(message: IaCdrMessage, recordName: String, now: Instant): Boolean {
        if (repository.existsByCompanyIdAndCdrUniqueIdAndRecordName(message.companyId, message.uniqueId, recordName)) {
            logger.info("${message.uniqueId} >> gravacao $recordName ja enfileirada, ignorando")
            return false
        }
        return try {
            repository.save(newTask(message, recordName, now))
            true
        } catch (e: DbActionExecutionException) {
            if (e.cause !is DuplicateKeyException) throw e
            // corrida entre o exists e o insert: outra entrega da mesma mensagem ganhou
            logger.info("${message.uniqueId} >> gravacao $recordName enfileirada em paralelo, ignorando")
            false
        }
    }

    private fun newTask(message: IaCdrMessage, recordName: String, now: Instant) = TranscriptionEntity(
        companyId = message.companyId,
        cdrUniqueId = message.uniqueId,
        recordName = recordName,
        status = TranscriptionStatus.PENDING,
        nextAttemptAt = now,
        callStartTime = Instant.ofEpochMilli(message.startTime),
        src = message.src,
        destination = message.destination,
        peer = message.peer,
        disposition = message.disposition,
        userfield = message.userfield,
        callDuration = message.duration,
        createdAt = now,
        updatedAt = now,
    )
}
```

- [ ] **Step 4: Rodar e ver passar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.TranscriptionTaskServiceTest"`
Expected: PASS (4 testes).

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: enfileiramento idempotente de tarefas de transcricao"
```

---

### Task 5: Consumer Kafka

**Files:**
- Modify: `build.gradle.kts` (dependência Kafka)
- Create: `kafka/KafkaSecurityConfig.kt`, `kafka/KafkaConsumerConfig.kt`, `kafka/IaCdrListener.kt`
- Test: `kafka/IaCdrListenerTest.kt`

**Interfaces:**
- Consumes: `IaCdrMessageParser.parse` (Task 3) e `TranscriptionTaskService.enqueue` (Task 4).
- Produces: bean `iaCdrListenerContainerFactory` e o listener `IaCdrListener.onMessage(payload: String)`.

- [ ] **Step 1: Adicionar a dependência**

Em `build.gradle.kts`, dentro de `dependencies`:
```kotlin
    implementation("org.springframework.kafka:spring-kafka")
```

- [ ] **Step 2: Escrever o teste que falha**

`src/test/kotlin/br/com/vipsolutions/vipia/kafka/IaCdrListenerTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.kafka

import br.com.vipsolutions.vipia.kafka.message.IaCdrMessage
import br.com.vipsolutions.vipia.transcription.TranscriptionTaskService
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import tools.jackson.module.kotlin.jacksonObjectMapper

class IaCdrListenerTest {

    private val taskService = mockk<TranscriptionTaskService>()
    private val listener = IaCdrListener(IaCdrMessageParser(jacksonObjectMapper()), taskService)

    @Test
    fun `payload valido enfileira as gravacoes`() {
        val captured = slot<IaCdrMessage>()
        every { taskService.enqueue(capture(captured)) } returns 1
        val payload = javaClass.getResourceAsStream("/kafka-contracts/ia-cdr-message.json")!!.bufferedReader().readText()

        listener.onMessage(payload)

        assertEquals("1772633126.1880", captured.captured.uniqueId)
    }

    @Test
    fun `payload malformado nao chama o servico`() {
        listener.onMessage("lixo")

        verify(exactly = 0) { taskService.enqueue(any()) }
    }
}
```

- [ ] **Step 3: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.kafka.IaCdrListenerTest"`
Expected: FAIL de compilação (`IaCdrListener` não existe).

- [ ] **Step 4: Implementar**

`kafka/KafkaSecurityConfig.kt` (mesmo do vip-worker):
```kotlin
package br.com.vipsolutions.vipia.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * Propriedades de autenticação SASL/SCRAM-SHA-512 compartilhadas pelas factories Kafka.
 * As factories usam mapas explícitos, então `spring.kafka.*` de segurança não é aplicado automaticamente.
 */
@Component
class KafkaSecurityConfig(
    @param:Value("\${spring.kafka.sasl.username}") private val username: String,
    @param:Value("\${spring.kafka.sasl.password}") private val password: String,
) {

    init {
        require(username.isNotBlank()) { "KAFKA_SASL_USERNAME não pode ser vazio" }
        require(password.isNotBlank()) { "KAFKA_SASL_PASSWORD não pode ser vazio" }
    }

    fun saslConfigs(): Map<String, Any> = mapOf(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SASL_PLAINTEXT",
        SaslConfigs.SASL_MECHANISM to "SCRAM-SHA-512",
        SaslConfigs.SASL_JAAS_CONFIG to "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"${escape(username)}\" password=\"${escape(password)}\";",
    )

    private fun escape(value: String) = value.replace("\\", "\\\\").replace("\"", "\\\"")
}
```

`kafka/KafkaConsumerConfig.kt`:
```kotlin
package br.com.vipsolutions.vipia.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

/**
 * O valor é consumido como String e convertido pelo IaCdrMessageParser: assim uma mensagem malformada é só
 * logada e descartada, sem depender de ErrorHandlingDeserializer.
 */
@EnableKafka
@Configuration
class KafkaConsumerConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val serverUrl: String,
    @param:Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
    @param:Value("\${spring.kafka.consumer.client-id}") private val clientId: String,
    private val kafkaSecurity: KafkaSecurityConfig,
) {

    @Bean
    fun iaCdrConsumerFactory(): ConsumerFactory<String, String> {
        val configs: Map<String, Any> = kafkaSecurity.saslConfigs() + mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to serverUrl,
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.CLIENT_ID_CONFIG to clientId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        )
        return DefaultKafkaConsumerFactory(configs)
    }

    @Bean
    fun iaCdrListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.setConsumerFactory(iaCdrConsumerFactory())
        return factory
    }
}
```

`kafka/IaCdrListener.kt`:
```kotlin
package br.com.vipsolutions.vipia.kafka

import br.com.vipsolutions.vipia.transcription.TranscriptionTaskService
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * Só persiste as tarefas: a transcrição roda depois no TranscriptionProcessor. Assim o offset é confirmado
 * rápido e a gravação que ainda não chegou ao S3 não trava a partição.
 */
@Component
class IaCdrListener(
    private val parser: IaCdrMessageParser,
    private val taskService: TranscriptionTaskService,
) {

    @KafkaListener(
        containerFactory = "iaCdrListenerContainerFactory",
        topics = ["\${spring.kafka.consumer.ia-cdr-topic}"],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "\${vip-ia.kafka.listener.auto-startup:true}",
    )
    fun onMessage(payload: String) {
        val message = parser.parse(payload) ?: return
        taskService.enqueue(message)
    }
}
```

- [ ] **Step 5: Rodar o teste e depois a suíte** (a suíte confirma que o contexto sobe sem broker, porque `auto-startup: false`)

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.kafka.IaCdrListenerTest"` and then `./tests.sh`
Expected: PASS em ambos.

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "feat: consumer kafka das CDRs com modulo de IA"
```

---

### Task 6: Download da gravação no S3

**Files:**
- Modify: `build.gradle.kts` (AWS SDK)
- Create: `recording/RecordingStorage.kt`, `recording/RecordingNotFoundException.kt`, `recording/AwsS3Config.kt`, `recording/S3RecordingStorage.kt`
- Test: `recording/S3RecordingStorageTest.kt`, `support/FakeRecordingStorage.kt`

**Interfaces:**
- Produces:
  - `interface RecordingStorage { fun download(companyId: String, recordName: String): ByteArray }`
  - `class RecordingNotFoundException(val key: String) : RuntimeException`
  - Teste: `class FakeRecordingStorage : RecordingStorage` com `val files: MutableMap<String, ByteArray>` (chave `"$companyId/$recordName"`).

- [ ] **Step 1: Adicionar a dependência**

Em `build.gradle.kts`, dentro de `dependencies`:
```kotlin
    implementation(platform("software.amazon.awssdk:bom:2.41.6"))
    implementation("software.amazon.awssdk:s3")
```

- [ ] **Step 2: Escrever o teste que falha**

`src/test/kotlin/br/com/vipsolutions/vipia/recording/S3RecordingStorageTest.kt`:
```kotlin
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
```

- [ ] **Step 3: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.recording.S3RecordingStorageTest"`
Expected: FAIL de compilação.

- [ ] **Step 4: Implementar**

`recording/RecordingStorage.kt`:
```kotlin
package br.com.vipsolutions.vipia.recording

/** Origem dos áudios das chamadas. Em produção é o S3; nos testes, um fake em memória. */
interface RecordingStorage {

    /** @throws RecordingNotFoundException se a gravação ainda não existe (o worker sobe depois da CDR). */
    fun download(companyId: String, recordName: String): ByteArray
}
```

`recording/RecordingNotFoundException.kt`:
```kotlin
package br.com.vipsolutions.vipia.recording

class RecordingNotFoundException(val key: String) : RuntimeException("Gravacao nao encontrada no storage: $key")
```

`recording/AwsS3Config.kt`:
```kotlin
package br.com.vipsolutions.vipia.recording

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

/** Credenciais vêm da cadeia padrão da AWS (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY no .env). */
@Configuration
class AwsS3Config(@param:Value("\${aws.s3.region}") private val region: String) {

    @Bean
    fun s3Client(): S3Client = S3Client.builder()
        .region(Region.of(region))
        .build()
}
```

`recording/S3RecordingStorage.kt`:
```kotlin
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
```

`src/test/kotlin/br/com/vipsolutions/vipia/support/FakeRecordingStorage.kt`:
```kotlin
package br.com.vipsolutions.vipia.support

import br.com.vipsolutions.vipia.recording.RecordingNotFoundException
import br.com.vipsolutions.vipia.recording.RecordingStorage

class FakeRecordingStorage : RecordingStorage {

    val files = mutableMapOf<String, ByteArray>()

    override fun download(companyId: String, recordName: String): ByteArray =
        files["$companyId/$recordName"] ?: throw RecordingNotFoundException("recordings/$companyId/$recordName.mp3")
}
```

- [ ] **Step 5: Rodar e ver passar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.recording.S3RecordingStorageTest"` and then `./tests.sh`
Expected: PASS em ambos.

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "feat: download das gravacoes no S3"
```

---

### Task 7: Provedor de transcrição Deepgram

**Files:**
- Modify: `build.gradle.kts` (SDK Deepgram)
- Create: `transcription/provider/TranscriptionProvider.kt`, `deepgram/DeepgramProperties.kt`, `deepgram/DeepgramConfig.kt`, `deepgram/DeepgramResponseMapper.kt`, `deepgram/DeepgramTranscriptionProvider.kt`
- Create: `src/test/resources/deepgram/response-with-speech.json`, `src/test/resources/deepgram/response-silence.json`
- Test: `deepgram/DeepgramResponseMapperTest.kt`, `deepgram/DeepgramTranscriptionProviderTest.kt`, `support/FakeTranscriptionProvider.kt`

**Interfaces:**
- Consumes: `TranscriptionProperties.language` (criado aqui, completado na Task 8; ver Step 4).
- Produces:
  - `interface TranscriptionProvider { fun transcribe(audio: ByteArray): TranscriptionResult }`
  - `data class TranscriptionResult(transcript: String, utterances: List<Utterance>, audioSeconds: Double, language: String, model: String, requestId: String)`
  - `data class Utterance(speaker: Int, startSeconds: Double, endSeconds: Double, content: String, confidence: Double?)`
  - `class TranscriptionFailedException(message: String, val retryable: Boolean, cause: Throwable? = null) : RuntimeException`
  - Teste: `class FakeTranscriptionProvider : TranscriptionProvider` com `var result: TranscriptionResult`, `var error: Exception?` e `var calls: Int`; mais `FakeTranscriptionProvider.sampleResult()`.

- [ ] **Step 1: Adicionar a dependência**

Em `build.gradle.kts`, dentro de `dependencies`:
```kotlin
    implementation("com.deepgram:deepgram-java-sdk:0.11.1")
```

- [ ] **Step 2: Fixtures de resposta da Deepgram**

`src/test/resources/deepgram/response-with-speech.json`:
```json
{
  "metadata": {
    "transaction_key": "deprecated",
    "request_id": "req-123",
    "sha256": "abc123",
    "created": "2026-10-15T12:00:00.000Z",
    "duration": 42.5,
    "channels": 1,
    "models": ["model-uuid"],
    "model_info": {"model-uuid": {"name": "general-nova-3", "version": "2025-01-01", "arch": "nova-3"}}
  },
  "results": {
    "channels": [
      {"alternatives": [{"transcript": "Olá, bom dia. Bom dia, em que posso ajudar?", "confidence": 0.98, "words": []}]}
    ],
    "utterances": [
      {"start": 0.5, "end": 1.75, "confidence": 0.97, "channel": 0, "transcript": "Olá, bom dia.", "words": [], "speaker": 0, "id": "u1"},
      {"start": 2.0, "end": 4.0, "confidence": 0.95, "channel": 0, "transcript": "Bom dia, em que posso ajudar?", "words": [], "speaker": 1, "id": "u2"}
    ]
  }
}
```

`src/test/resources/deepgram/response-silence.json`:
```json
{
  "metadata": {
    "transaction_key": "deprecated",
    "request_id": "req-silence",
    "sha256": "def456",
    "created": "2026-10-15T12:00:00.000Z",
    "duration": 7.0,
    "channels": 1,
    "models": ["model-uuid"],
    "model_info": {"model-uuid": {"name": "general-nova-3", "version": "2025-01-01", "arch": "nova-3"}}
  },
  "results": {
    "channels": [
      {"alternatives": [{"transcript": "", "confidence": 0.0, "words": []}]}
    ],
    "utterances": []
  }
}
```

- [ ] **Step 3: Escrever os testes que falham**

`src/test/kotlin/br/com/vipsolutions/vipia/deepgram/DeepgramResponseMapperTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.deepgram

import br.com.vipsolutions.vipia.transcription.provider.Utterance
import com.deepgram.core.ObjectMappers
import com.deepgram.types.ListenV1Response
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DeepgramResponseMapperTest {

    private fun load(name: String): ListenV1Response =
        ObjectMappers.JSON_MAPPER.readValue(
            javaClass.getResourceAsStream("/deepgram/$name")!!.readAllBytes(),
            ListenV1Response::class.java,
        )

    @Test
    fun `mapeia texto, falas por locutor, duracao e request id`() {
        val result = DeepgramResponseMapper.toResult(load("response-with-speech.json"), "pt-BR")

        assertEquals("Olá, bom dia. Bom dia, em que posso ajudar?", result.transcript)
        assertEquals(42.5, result.audioSeconds)
        assertEquals("req-123", result.requestId)
        assertEquals("nova-3", result.model)
        assertEquals("pt-BR", result.language)
        assertEquals(
            listOf(
                Utterance(speaker = 0, startSeconds = 0.5, endSeconds = 1.75, content = "Olá, bom dia.", confidence = 0.97f.toDouble()),
                Utterance(speaker = 1, startSeconds = 2.0, endSeconds = 4.0, content = "Bom dia, em que posso ajudar?", confidence = 0.95f.toDouble()),
            ),
            result.utterances,
        )
    }

    @Test
    fun `resposta sem fala gera resultado vazio`() {
        val result = DeepgramResponseMapper.toResult(load("response-silence.json"), "pt-BR")

        assertEquals("", result.transcript)
        assertTrue(result.utterances.isEmpty())
        assertEquals(7.0, result.audioSeconds)
    }
}
```

`src/test/kotlin/br/com/vipsolutions/vipia/deepgram/DeepgramTranscriptionProviderTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.deepgram

import br.com.vipsolutions.vipia.transcription.TranscriptionProperties
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionFailedException
import com.deepgram.DeepgramClient
import com.deepgram.core.DeepgramApiException
import com.deepgram.core.DeepgramHttpException
import com.deepgram.core.ObjectMappers
import com.deepgram.errors.BadRequestError
import com.deepgram.resources.listen.v1.media.requests.MediaTranscribeRequestOctetStream
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeRequestModel
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeResponse
import com.deepgram.types.ListenV1Response
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class DeepgramTranscriptionProviderTest {

    private val client = mockk<DeepgramClient>()
    private val properties = TranscriptionProperties(
        pollInterval = Duration.ofSeconds(10),
        batchSize = 20,
        maxConcurrency = 4,
        maxAttempts = 6,
        retryBaseDelay = Duration.ofMinutes(1),
        language = "pt-BR",
    )
    private val provider = DeepgramTranscriptionProvider(client, properties)
    private val audio = byteArrayOf(9, 9, 9)

    private fun speechResponse(): MediaTranscribeResponse = MediaTranscribeResponse.of(
        ObjectMappers.JSON_MAPPER.readValue(
            javaClass.getResourceAsStream("/deepgram/response-with-speech.json")!!.readAllBytes(),
            ListenV1Response::class.java,
        )
    )

    private fun givenDeepgramThrows(error: Exception) {
        every { client.listen().v1().media().transcribeFile(any<MediaTranscribeRequestOctetStream>()) } throws error
    }

    @Test
    fun `envia o audio com nova-3, pt-BR, diarizacao e utterances`() {
        val request = slot<MediaTranscribeRequestOctetStream>()
        every { client.listen().v1().media().transcribeFile(capture(request)) } returns speechResponse()

        val result = provider.transcribe(audio)

        assertEquals("req-123", result.requestId)
        assertArrayEquals(audio, request.captured.body)
        assertEquals(MediaTranscribeRequestModel.NOVA3, request.captured.model.get())
        assertEquals("pt-BR", request.captured.language.get())
        assertTrue(request.captured.diarize.get())
        assertTrue(request.captured.utterances.get())
        assertTrue(request.captured.punctuate.get())
        assertTrue(request.captured.smartFormat.get())
    }

    @Test
    fun `erro 5xx e retentavel`() {
        givenDeepgramThrows(DeepgramHttpException("indisponivel", 503, "Service Unavailable"))

        assertTrue(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }

    @Test
    fun `erro 429 e retentavel`() {
        givenDeepgramThrows(DeepgramHttpException("rate limit", 429, "Too Many Requests"))

        assertTrue(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }

    @Test
    fun `erro 400 nao e retentavel`() {
        givenDeepgramThrows(BadRequestError("audio corrompido"))

        assertFalse(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }

    @Test
    fun `erro 401 nao e retentavel`() {
        givenDeepgramThrows(DeepgramHttpException("nao autorizado", 401, "Invalid credentials"))

        assertFalse(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }

    @Test
    fun `falha de rede e retentavel`() {
        givenDeepgramThrows(DeepgramApiException("timeout"))

        assertTrue(assertThrows<TranscriptionFailedException> { provider.transcribe(audio) }.retryable)
    }
}
```

- [ ] **Step 4: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.deepgram.*"`
Expected: FAIL de compilação.

- [ ] **Step 5: Implementar**

`transcription/TranscriptionProperties.kt` (é completado na Task 8, mas já nasce com todos os campos):
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "vip-ia.transcription")
data class TranscriptionProperties(
    /** Intervalo entre varreduras da fila. */
    val pollInterval: Duration,
    /** Máximo de tarefas buscadas por varredura. */
    val batchSize: Int,
    /** Máximo de chamadas simultâneas à Deepgram. */
    val maxConcurrency: Int,
    /** Tentativas antes de FAILED. */
    val maxAttempts: Int,
    /** Base do backoff exponencial: base * 2^(tentativas-1). */
    val retryBaseDelay: Duration,
    /** Idioma enviado à Deepgram (BCP-47). */
    val language: String,
)
```

`transcription/provider/TranscriptionProvider.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription.provider

/** Serviço de speech-to-text. Em produção é a Deepgram; nos testes, um fake. */
interface TranscriptionProvider {

    /** @throws TranscriptionFailedException com retryable indicando se vale tentar de novo. */
    fun transcribe(audio: ByteArray): TranscriptionResult
}

data class TranscriptionResult(
    val transcript: String,
    val utterances: List<Utterance>,
    val audioSeconds: Double,
    val language: String,
    val model: String,
    val requestId: String,
)

data class Utterance(
    val speaker: Int,
    val startSeconds: Double,
    val endSeconds: Double,
    val content: String,
    val confidence: Double?,
)

class TranscriptionFailedException(
    message: String,
    val retryable: Boolean,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
```

`deepgram/DeepgramProperties.kt`:
```kotlin
package br.com.vipsolutions.vipia.deepgram

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "deepgram")
data class DeepgramProperties(
    val apiKey: String,
    /** Timeout da requisição de transcrição (gravações longas demoram). */
    val timeoutSeconds: Int,
)
```

`deepgram/DeepgramConfig.kt`:
```kotlin
package br.com.vipsolutions.vipia.deepgram

import com.deepgram.DeepgramClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class DeepgramConfig(private val properties: DeepgramProperties) {

    /** maxRetries(0): o retry com backoff é feito pelo TranscriptionProcessor, não pelo SDK. */
    @Bean
    fun deepgramClient(): DeepgramClient = DeepgramClient.builder()
        .apiKey(properties.apiKey)
        .timeout(properties.timeoutSeconds)
        .maxRetries(0)
        .build()
}
```

`deepgram/DeepgramResponseMapper.kt`:
```kotlin
package br.com.vipsolutions.vipia.deepgram

import br.com.vipsolutions.vipia.transcription.provider.TranscriptionResult
import br.com.vipsolutions.vipia.transcription.provider.Utterance
import com.deepgram.types.ListenV1Response

/** Converte a resposta do SDK (cheia de Optional) no nosso TranscriptionResult. */
object DeepgramResponseMapper {

    const val MODEL = "nova-3"

    fun toResult(response: ListenV1Response, language: String): TranscriptionResult {
        val utterances = response.results.utterances.orElse(emptyList())
            .map { item ->
                Utterance(
                    speaker = item.speaker.orElse(0),
                    startSeconds = item.start.orElse(0f).toDouble(),
                    endSeconds = item.end.orElse(0f).toDouble(),
                    content = item.transcript.orElse("").trim(),
                    confidence = item.confidence.map { it.toDouble() }.orElse(null),
                )
            }
            .filter { it.content.isNotEmpty() }
        val transcript = response.results.channels.firstOrNull()
            ?.alternatives?.orElse(emptyList())?.firstOrNull()
            ?.transcript?.orElse("")
            ?: ""
        return TranscriptionResult(
            transcript = transcript.trim(),
            utterances = utterances,
            audioSeconds = response.metadata.duration,
            language = language,
            model = MODEL,
            requestId = response.metadata.requestId,
        )
    }
}
```

`deepgram/DeepgramTranscriptionProvider.kt`:
```kotlin
package br.com.vipsolutions.vipia.deepgram

import br.com.vipsolutions.vipia.transcription.TranscriptionProperties
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionFailedException
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionProvider
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionResult
import com.deepgram.DeepgramClient
import com.deepgram.core.DeepgramApiException
import com.deepgram.core.DeepgramHttpException
import com.deepgram.resources.listen.v1.media.requests.MediaTranscribeRequestOctetStream
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeRequestModel
import com.deepgram.resources.listen.v1.media.types.MediaTranscribeResponse
import com.deepgram.types.ListenV1AcceptedResponse
import com.deepgram.types.ListenV1Response
import org.springframework.stereotype.Component

@Component
class DeepgramTranscriptionProvider(
    private val client: DeepgramClient,
    private val properties: TranscriptionProperties,
) : TranscriptionProvider {

    override fun transcribe(audio: ByteArray): TranscriptionResult {
        val request = MediaTranscribeRequestOctetStream.builder()
            .body(audio)
            .model(MediaTranscribeRequestModel.NOVA3)
            .language(properties.language)
            .diarize(true)
            .utterances(true)
            .punctuate(true)
            .smartFormat(true)
            .build()
        val response = try {
            client.listen().v1().media().transcribeFile(request)
        } catch (e: DeepgramHttpException) {
            val status = e.statusCode()
            val retryable = status == 408 || status == 429 || status >= 500
            throw TranscriptionFailedException("Deepgram HTTP $status: ${e.body()}", retryable, e)
        } catch (e: DeepgramApiException) {
            throw TranscriptionFailedException("Falha de comunicacao com a Deepgram: ${e.message}", true, e)
        }
        return response.visit(object : MediaTranscribeResponse.Visitor<TranscriptionResult> {
            override fun visit(value: ListenV1Response): TranscriptionResult =
                DeepgramResponseMapper.toResult(value, properties.language)

            override fun visit(value: ListenV1AcceptedResponse): TranscriptionResult =
                throw TranscriptionFailedException("Deepgram respondeu em modo callback sem callback configurado", false)
        })
    }
}
```

`src/test/kotlin/br/com/vipsolutions/vipia/support/FakeTranscriptionProvider.kt`:
```kotlin
package br.com.vipsolutions.vipia.support

import br.com.vipsolutions.vipia.transcription.provider.TranscriptionProvider
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionResult
import br.com.vipsolutions.vipia.transcription.provider.Utterance

class FakeTranscriptionProvider : TranscriptionProvider {

    var result: TranscriptionResult = sampleResult()
    var error: Exception? = null
    var calls = 0

    override fun transcribe(audio: ByteArray): TranscriptionResult {
        calls++
        error?.let { throw it }
        return result
    }

    companion object {
        fun sampleResult() = TranscriptionResult(
            transcript = "Olá, bom dia. Bom dia, em que posso ajudar?",
            utterances = listOf(
                Utterance(speaker = 0, startSeconds = 0.5, endSeconds = 1.75, content = "Olá, bom dia.", confidence = 0.97),
                Utterance(speaker = 1, startSeconds = 2.0, endSeconds = 4.0, content = "Bom dia, em que posso ajudar?", confidence = 0.95),
            ),
            audioSeconds = 42.5,
            language = "pt-BR",
            model = "nova-3",
            requestId = "req-123",
        )
    }
}
```

- [ ] **Step 6: Rodar e ver passar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.deepgram.*"` and then `./tests.sh`
Expected: PASS em ambos.

Se o `ObjectMappers.JSON_MAPPER` recusar a fixture por campo obrigatório ausente, complete a fixture com o campo citado na mensagem de erro. Não afrouxe o mapper.

- [ ] **Step 7: Commit**

```bash
git add .
git commit -m "feat: provedor de transcricao Deepgram"
```

---

### Task 8: Processador de transcrições (retry, conclusão e consumo)

**Files:**
- Create: `transcription/RetryPolicy.kt`, `transcription/TranscriptionCompletionService.kt`, `transcription/TranscriptionProcessor.kt`, `usage/UsageService.kt`
- Test: `transcription/RetryPolicyTest.kt`, `transcription/TranscriptionProcessorTest.kt`

**Interfaces:**
- Consumes: `TranscriptionRepository` e `TranscriptionUtteranceRepository` (Task 2), `UsageRecordRepository` (Task 2), `RecordingStorage` e `RecordingNotFoundException` (Task 6), `TranscriptionProvider`, `TranscriptionResult` e `TranscriptionFailedException` (Task 7), `TranscriptionProperties` (Task 7).
- Produces:
  - `RetryPolicy(properties)`: `fun nextAttemptAt(attempts: Int, now: Instant): Instant` e `fun shouldGiveUp(attempts: Int): Boolean`.
  - `UsageService(repository)`: `fun recordTranscription(companyId: String, transcriptionId: Long, audioSeconds: Double, model: String, now: Instant)`.
  - `TranscriptionCompletionService`: `fun complete(task: TranscriptionEntity, result: TranscriptionResult)` (`@Transactional`).
  - `TranscriptionProcessor`: `fun processDueTasks(): Int` e `fun processOne(task: TranscriptionEntity)`.

- [ ] **Step 1: Escrever os testes que falham**

`src/test/kotlin/br/com/vipsolutions/vipia/transcription/RetryPolicyTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class RetryPolicyTest {

    private val policy = RetryPolicy(
        TranscriptionProperties(Duration.ofSeconds(10), 20, 4, 6, Duration.ofMinutes(1), "pt-BR")
    )
    private val now = Instant.parse("2026-10-15T12:00:00Z")

    @Test
    fun `backoff exponencial a partir da base`() {
        assertEquals(now.plus(Duration.ofMinutes(1)), policy.nextAttemptAt(1, now))
        assertEquals(now.plus(Duration.ofMinutes(2)), policy.nextAttemptAt(2, now))
        assertEquals(now.plus(Duration.ofMinutes(16)), policy.nextAttemptAt(5, now))
    }

    @Test
    fun `desiste ao atingir o maximo de tentativas`() {
        assertFalse(policy.shouldGiveUp(5))
        assertTrue(policy.shouldGiveUp(6))
    }
}
```

`src/test/kotlin/br/com/vipsolutions/vipia/transcription/TranscriptionProcessorTest.kt`:
```kotlin
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.RetryPolicyTest" --tests "br.com.vipsolutions.vipia.transcription.TranscriptionProcessorTest"`
Expected: FAIL de compilação.

- [ ] **Step 3: Implementar**

`transcription/RetryPolicy.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.springframework.stereotype.Component
import java.time.Instant

/** Backoff exponencial: com base 1min e 6 tentativas (esperas de 1+2+4+8+16min), desiste ~31min após a 1ª falha. */
@Component
class RetryPolicy(private val properties: TranscriptionProperties) {

    /** @param attempts tentativas que já falharam (>= 1). */
    fun nextAttemptAt(attempts: Int, now: Instant): Instant {
        val exponent = (attempts - 1).coerceIn(0, 20)
        return now.plus(properties.retryBaseDelay.multipliedBy(1L shl exponent))
    }

    fun shouldGiveUp(attempts: Int): Boolean = attempts >= properties.maxAttempts
}
```

`usage/UsageService.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage

import org.springframework.stereotype.Service
import java.time.Instant

@Service
class UsageService(private val repository: UsageRecordRepository) {

    /** Custo fica NULL: a Deepgram não devolve preço; o relatório financeiro aplica o valor por minuto. */
    fun recordTranscription(companyId: String, transcriptionId: Long, audioSeconds: Double, model: String, now: Instant) {
        repository.save(
            UsageRecordEntity(
                companyId = companyId,
                usageType = UsageType.TRANSCRIPTION,
                referenceId = transcriptionId,
                provider = UsageProvider.DEEPGRAM,
                model = model,
                audioSeconds = audioSeconds,
                createdAt = now,
            )
        )
    }
}
```

`transcription/TranscriptionCompletionService.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.transcription.provider.TranscriptionResult
import br.com.vipsolutions.vipia.usage.UsageService
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Clock

/** Grava resultado + falas + consumo numa única transação: ou tudo, ou nada. */
@Service
class TranscriptionCompletionService(
    private val repository: TranscriptionRepository,
    private val utteranceRepository: TranscriptionUtteranceRepository,
    private val usageService: UsageService,
    private val clock: Clock,
) {

    @Transactional
    fun complete(task: TranscriptionEntity, result: TranscriptionResult) {
        val id = requireNotNull(task.id) { "Tarefa sem id" }
        val now = clock.instant()
        repository.markDone(id, result.audioSeconds, result.language, result.model, result.requestId, result.transcript, now)
        utteranceRepository.deleteByTranscriptionId(id)
        utteranceRepository.saveAll(
            result.utterances.map {
                TranscriptionUtteranceEntity(
                    transcriptionId = id,
                    speaker = it.speaker,
                    startSeconds = it.startSeconds,
                    endSeconds = it.endSeconds,
                    content = it.content,
                    confidence = it.confidence,
                )
            }
        )
        usageService.recordTranscription(task.companyId, id, result.audioSeconds, result.model, now)
    }
}
```

`transcription/TranscriptionProcessor.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.recording.RecordingNotFoundException
import br.com.vipsolutions.vipia.recording.RecordingStorage
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionFailedException
import br.com.vipsolutions.vipia.transcription.provider.TranscriptionProvider
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

/**
 * Consome a fila de transcrições: busca tarefas vencidas, reivindica cada uma atomicamente (claim), baixa o
 * áudio, transcreve e conclui. Falha vira RETRY (com backoff) ou FAILED. Roda cada tarefa numa virtual thread,
 * limitado por maxConcurrency para controlar custo e rate limit da Deepgram.
 */
@Service
class TranscriptionProcessor(
    private val repository: TranscriptionRepository,
    private val storage: RecordingStorage,
    private val provider: TranscriptionProvider,
    private val completionService: TranscriptionCompletionService,
    private val retryPolicy: RetryPolicy,
    private val properties: TranscriptionProperties,
    private val clock: Clock,
) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun processDueTasks(): Int {
        val due = repository.findDue(clock.instant(), properties.batchSize)
        if (due.isEmpty()) return 0
        val permits = Semaphore(properties.maxConcurrency)
        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            due.forEach { task ->
                executor.submit {
                    permits.acquire()
                    try {
                        processOne(task)
                    } finally {
                        permits.release()
                    }
                }
            }
        }
        return due.size
    }

    fun processOne(task: TranscriptionEntity) {
        val id = requireNotNull(task.id) { "Tarefa sem id" }
        if (repository.claim(id, clock.instant()) == 0) return
        try {
            val audio = storage.download(task.companyId, task.recordName)
            val result = provider.transcribe(audio)
            completionService.complete(task, result)
            logger.info("${task.cdrUniqueId} >> gravacao ${task.recordName} transcrita (${result.audioSeconds}s)")
        } catch (e: RecordingNotFoundException) {
            registerFailure(task, e.message ?: "Gravacao nao encontrada", retryable = true)
        } catch (e: TranscriptionFailedException) {
            registerFailure(task, e.message ?: "Falha na transcricao", e.retryable)
        } catch (e: Exception) {
            logger.error("${task.cdrUniqueId} >> erro inesperado ao transcrever ${task.recordName}", e)
            registerFailure(task, e.message ?: e.javaClass.simpleName, retryable = true)
        }
    }

    private fun registerFailure(task: TranscriptionEntity, error: String, retryable: Boolean) {
        val id = requireNotNull(task.id)
        val attempts = task.attempts + 1
        val now = clock.instant()
        val message = error.take(1000)
        if (!retryable || retryPolicy.shouldGiveUp(attempts)) {
            repository.markFailed(id, attempts, message, now)
            logger.warn("${task.cdrUniqueId} >> transcricao de ${task.recordName} FALHOU apos $attempts tentativa(s): $message")
        } else {
            val next = retryPolicy.nextAttemptAt(attempts, now)
            repository.markRetry(id, attempts, next, message, now)
            logger.info("${task.cdrUniqueId} >> transcricao de ${task.recordName} reagendada para $next: $message")
        }
    }
}
```

- [ ] **Step 4: Rodar e ver passar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.*"` and then `./tests.sh`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: processador de transcricoes com retry, conclusao transacional e consumo"
```

---

### Task 9: Agendamento e recuperação de tarefas presas

**Files:**
- Create: `config/SchedulingConfig.kt`, `transcription/TranscriptionScheduler.kt`, `transcription/StuckTranscriptionRecovery.kt`
- Test: `transcription/StuckTranscriptionRecoveryTest.kt`, `transcription/TranscriptionSchedulerTest.kt`

**Interfaces:**
- Consumes: `TranscriptionProcessor.processDueTasks()` (Task 8) e `TranscriptionRepository.resetStuck(now)` (Task 2).
- Produces: `TranscriptionScheduler.run()` (executado só se `vip-ia.scheduling.enabled=true`) e `StuckTranscriptionRecovery.recover(): Int`.

- [ ] **Step 1: Escrever os testes que falham**

`src/test/kotlin/br/com/vipsolutions/vipia/transcription/StuckTranscriptionRecoveryTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.support.Fixtures
import br.com.vipsolutions.vipia.support.IntegrationTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

class StuckTranscriptionRecoveryTest : IntegrationTest() {

    @Autowired lateinit var recovery: StuckTranscriptionRecovery
    @Autowired lateinit var repository: TranscriptionRepository

    @Test
    fun `tarefas PROCESSING voltam para RETRY e as demais nao mudam`() {
        val stuck = repository.save(Fixtures.task(recordName = "a", status = TranscriptionStatus.PROCESSING)).id!!
        val done = repository.save(Fixtures.task(recordName = "b", status = TranscriptionStatus.DONE)).id!!

        assertEquals(1, recovery.recover())

        assertEquals(TranscriptionStatus.RETRY, repository.findById(stuck).get().status)
        assertEquals(TranscriptionStatus.DONE, repository.findById(done).get().status)
    }
}
```

`src/test/kotlin/br/com/vipsolutions/vipia/transcription/TranscriptionSchedulerTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import br.com.vipsolutions.vipia.support.IntegrationTest
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext

class TranscriptionSchedulerTest : IntegrationTest() {

    @Autowired lateinit var context: ApplicationContext

    @Test
    fun `scheduler nao e registrado quando vip-ia scheduling enabled e false`() {
        assertTrue(context.getBeansOfType(TranscriptionScheduler::class.java).isEmpty())
    }

    @Test
    fun `run delega ao processador e engole excecoes`() {
        val processor = mockk<TranscriptionProcessor>()
        every { processor.processDueTasks() } throws IllegalStateException("banco fora")

        assertDoesNotThrow { TranscriptionScheduler(processor).run() }
        verify(exactly = 1) { processor.processDueTasks() }
    }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.StuckTranscriptionRecoveryTest" --tests "br.com.vipsolutions.vipia.transcription.TranscriptionSchedulerTest"`
Expected: FAIL de compilação.

- [ ] **Step 3: Implementar**

`config/SchedulingConfig.kt`:
```kotlin
package br.com.vipsolutions.vipia.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

/** Desligado nos testes (vip-ia.scheduling.enabled=false) para os processadores só rodarem quando chamados. */
@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "vip-ia.scheduling", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class SchedulingConfig
```

`transcription/TranscriptionScheduler.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(prefix = "vip-ia.scheduling", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class TranscriptionScheduler(private val processor: TranscriptionProcessor) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    @Scheduled(fixedDelayString = "\${vip-ia.transcription.poll-interval}")
    fun run() {
        try {
            processor.processDueTasks()
        } catch (e: Exception) {
            logger.error("Erro na varredura da fila de transcricoes", e)
        }
    }
}
```

`transcription/StuckTranscriptionRecovery.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription

import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Clock

/**
 * No boot, tarefas em PROCESSING são de uma execução que caiu no meio: voltam para RETRY.
 * Premissa: uma única instância do vip-ia por banco (com várias instâncias, isto roubaria tarefas em andamento).
 */
@Component
class StuckTranscriptionRecovery(
    private val repository: TranscriptionRepository,
    private val clock: Clock,
) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun recover(): Int {
        val recovered = repository.resetStuck(clock.instant())
        if (recovered > 0) logger.warn("$recovered transcricao(oes) presa(s) em PROCESSING devolvida(s) para RETRY")
        return recovered
    }
}
```

- [ ] **Step 4: Rodar e ver passar**

Run: `./tests.sh`
Expected: PASS.

- [ ] **Step 5: Atualizar o `AGENTS.md`** com a premissa de instância única. Na seção "Fila de tarefas no banco", adicione a linha:
```markdown
- **Instância única:** o vip-ia roda com **uma** instância por banco — o `StuckTranscriptionRecovery` devolve
  para `RETRY` tudo que está em `PROCESSING` no boot. Escalar horizontalmente exige rever isso.
```

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "feat: agendamento da fila e recuperacao de tarefas presas no boot"
```

---

### Task 10: Segurança (JWT do manager)

**Files:**
- Modify: `build.gradle.kts` (security + java-jwt)
- Create: `auth/JwtService.kt`, `auth/UserPrincipal.kt`, `auth/FilterToken.kt`, `auth/SecurityConfig.kt`
- Test: `support/TestTokens.kt`, `auth/JwtServiceTest.kt`, `auth/SecurityConfigTest.kt`

**Interfaces:**
- Produces:
  - `UserPrincipal(id: Long, name: String, email: String, companyId: String, tenantId: Long, roles: List<String>, managingCompany: Map<String, Any>?)`, que é o principal da autenticação (`@AuthenticationPrincipal user: UserPrincipal`).
  - `JwtService.validate(token: String): DecodedJWT?`.
  - Teste: `TestTokens.token(companyId = "100023", roles = listOf("ROLE_COMPANY_USER"), ...)` e `TestTokens.bearer(...)`, que devolve `"Bearer <jwt>"`.

- [ ] **Step 1: Adicionar as dependências**

Em `build.gradle.kts`, dentro de `dependencies`:
```kotlin
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("com.auth0:java-jwt:4.5.0")
    testImplementation("org.springframework.boot:spring-boot-starter-security-test")
```

- [ ] **Step 2: Helper de tokens de teste**

`src/test/kotlin/br/com/vipsolutions/vipia/support/TestTokens.kt`:
```kotlin
package br.com.vipsolutions.vipia.support

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import java.time.Instant

/** Gera tokens no mesmo formato do JWTService do vip-pabx-manager. */
object TestTokens {

    const val SECRET = "test-secret"

    fun token(
        companyId: String = "100023",
        roles: List<String> = listOf("ROLE_COMPANY_USER"),
        secret: String = SECRET,
        issuer: String = "VIP API",
        expiresAt: Instant = Instant.now().plusSeconds(300),
        managingCompany: Map<String, Any>? = null,
    ): String {
        val builder = JWT.create()
            .withIssuer(issuer)
            .withSubject("usuario@teste.com")
            .withClaim("id", 1L)
            .withClaim("name", "Usuario Teste")
            .withClaim("companyId", companyId)
            .withClaim("tenantId", 1L)
            .withClaim("isPasswordCreated", true)
            .withClaim("roles", roles)
            .withExpiresAt(expiresAt)
        managingCompany?.let { builder.withClaim("managingCompany", it) }
        return builder.sign(Algorithm.HMAC512(secret))
    }

    fun bearer(
        companyId: String = "100023",
        roles: List<String> = listOf("ROLE_COMPANY_USER"),
    ): String = "Bearer ${token(companyId = companyId, roles = roles)}"
}
```

- [ ] **Step 3: Escrever os testes que falham**

`src/test/kotlin/br/com/vipsolutions/vipia/auth/JwtServiceTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.auth

import br.com.vipsolutions.vipia.support.TestTokens
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Instant

class JwtServiceTest {

    private val service = JwtService(TestTokens.SECRET)

    @Test
    fun `token valido do manager vira UserPrincipal`() {
        val decoded = service.validate(TestTokens.token(companyId = "100023", roles = listOf("ROLE_COMPANY_ADMIN")))

        assertNotNull(decoded)
        val principal = UserPrincipal(decoded!!)
        assertEquals("100023", principal.companyId)
        assertEquals(listOf("ROLE_COMPANY_ADMIN"), principal.roles)
        assertEquals("usuario@teste.com", principal.email)
        assertNull(principal.managingCompany)
    }

    @Test
    fun `token de gerenciamento usa o companyId da empresa gerenciada`() {
        val token = TestTokens.token(companyId = "200000", managingCompany = mapOf("companyId" to "200000", "name" to "Cliente"))

        val principal = UserPrincipal(service.validate(token)!!)

        assertEquals("200000", principal.companyId)
        assertEquals("Cliente", principal.managingCompany!!["name"])
    }

    @Test
    fun `assinatura errada, issuer errado ou expirado sao rejeitados`() {
        assertNull(service.validate(TestTokens.token(secret = "outro-segredo")))
        assertNull(service.validate(TestTokens.token(issuer = "OUTRO")))
        assertNull(service.validate(TestTokens.token(expiresAt = Instant.now().minusSeconds(60))))
        assertNull(service.validate("nao-e-jwt"))
    }
}
```

`src/test/kotlin/br/com/vipsolutions/vipia/auth/SecurityConfigTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.auth

import br.com.vipsolutions.vipia.support.IntegrationTest
import br.com.vipsolutions.vipia.support.TestTokens
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

/**
 * Usa uma rota inexistente: sem auth deve dar 401; com token válido passa da segurança e cai no 404.
 */
class SecurityConfigTest : IntegrationTest() {

    @Test
    fun `sem token retorna 401`() {
        mockMvc.perform(get("/rota-inexistente")).andExpect(status().isUnauthorized)
    }

    @Test
    fun `token invalido retorna 401`() {
        mockMvc.perform(get("/rota-inexistente").header("Authorization", "Bearer ${TestTokens.token(secret = "x")}"))
            .andExpect(status().isUnauthorized)
    }

    @Test
    fun `token valido passa pela seguranca`() {
        mockMvc.perform(get("/rota-inexistente").header("Authorization", TestTokens.bearer()))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `actuator continua aberto`() {
        mockMvc.perform(get("/actuator/health")).andExpect(status().isOk)
    }
}
```

- [ ] **Step 4: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.auth.*"`
Expected: FAIL de compilação (`JwtService` e `UserPrincipal` não existem).

- [ ] **Step 5: Implementar**

`auth/JwtService.kt`:
```kotlin
package br.com.vipsolutions.vipia.auth

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.exceptions.JWTVerificationException
import com.auth0.jwt.interfaces.DecodedJWT
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

/** Só valida: quem emite o token é o vip-pabx-manager (mesmo segredo e issuer). */
@Service
class JwtService(@param:Value("\${auth.jwt.secret}") private val secret: String) {

    private val verifier = JWT.require(Algorithm.HMAC512(secret))
        .withIssuer("VIP API")
        .build()

    fun validate(token: String): DecodedJWT? = try {
        verifier.verify(token)
    } catch (e: JWTVerificationException) {
        null
    }
}
```

`auth/UserPrincipal.kt`:
```kotlin
package br.com.vipsolutions.vipia.auth

import com.auth0.jwt.interfaces.DecodedJWT

/**
 * Usuário autenticado, extraído do JWT do manager. Em token de gerenciamento (suporte "virando" a empresa),
 * companyId já vem com a empresa gerenciada.
 */
data class UserPrincipal(
    val id: Long,
    val name: String,
    val email: String,
    val companyId: String,
    val tenantId: Long,
    val roles: List<String>,
    val managingCompany: Map<String, Any>? = null,
) {

    constructor(decodedJWT: DecodedJWT) : this(
        id = decodedJWT.getClaim("id").asLong(),
        name = decodedJWT.getClaim("name").asString(),
        email = decodedJWT.subject,
        companyId = decodedJWT.getClaim("companyId").asString(),
        tenantId = decodedJWT.getClaim("tenantId").asLong(),
        roles = decodedJWT.getClaim("roles").asList(String::class.java),
        managingCompany = decodedJWT.getClaim("managingCompany").let { if (it.isMissing || it.isNull) null else it.asMap() },
    )
}
```

`auth/FilterToken.kt`:
```kotlin
package br.com.vipsolutions.vipia.auth

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

@Component
class FilterToken(private val jwtService: JwtService) : OncePerRequestFilter() {

    private val tokenPrefix = "Bearer "

    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, filterChain: FilterChain) {
        val header = request.getHeader("Authorization")
        if (header != null && header.startsWith(tokenPrefix)) {
            authenticate(header.removePrefix(tokenPrefix))
        }
        filterChain.doFilter(request, response)
    }

    private fun authenticate(token: String) {
        val decoded = jwtService.validate(token) ?: return
        val principal = try {
            UserPrincipal(decoded)
        } catch (e: Exception) {
            // token assinado mas sem os claims esperados: trata como não autenticado (401), não como 500
            return
        }
        SecurityContextHolder.getContext().authentication =
            UsernamePasswordAuthenticationToken(principal, null, principal.roles.map(::SimpleGrantedAuthority))
    }
}
```

`auth/SecurityConfig.kt`:
```kotlin
package br.com.vipsolutions.vipia.auth

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.web.SecurityFilterChain
import org.springframework.security.web.authentication.HttpStatusEntryPoint
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.CorsConfigurationSource
import org.springframework.web.cors.UrlBasedCorsConfigurationSource

@Configuration
@EnableWebSecurity
@EnableMethodSecurity
class SecurityConfig(private val filterToken: FilterToken) {

    @Bean
    fun securityFilterChain(http: HttpSecurity): SecurityFilterChain = http
        .csrf { it.disable() }
        .cors { }
        .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
        .authorizeHttpRequests {
            it.requestMatchers("/actuator/health", "/actuator/prometheus").permitAll()
                .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                .anyRequest().authenticated()
        }
        .exceptionHandling { it.authenticationEntryPoint(HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)) }
        .addFilterBefore(filterToken, UsernamePasswordAuthenticationFilter::class.java)
        .build()

    /** CORS só para a API usada pelo frontend; actuator e /internal não são chamados do navegador. */
    @Bean
    fun corsConfigurationSource(): CorsConfigurationSource {
        val config = CorsConfiguration().apply {
            allowedOrigins = listOf("*")
            allowedMethods = listOf("GET", "POST", "PUT", "DELETE", "OPTIONS")
            allowedHeaders = listOf("*")
        }
        return UrlBasedCorsConfigurationSource().apply {
            registerCorsConfiguration("/transcriptions/**", config)
        }
    }
}
```

- [ ] **Step 6: Rodar e ver passar**

Run: `./tests.sh`
Expected: PASS. O `ActuatorEndpointsTest` da Task 1 continua passando sem token.

- [ ] **Step 7: Commit**

```bash
git add .
git commit -m "feat: autenticacao pelo JWT do manager"
```

---

### Task 11: API de transcrições

**Files:**
- Create: `common/PageResponse.kt`, `transcription/api/dto/TranscriptionDtos.kt`, `transcription/api/TranscriptionQueryRepository.kt`, `transcription/api/TranscriptionApiService.kt`, `transcription/api/TranscriptionController.kt`
- Test: `transcription/api/TranscriptionControllerTest.kt`

**Interfaces:**
- Consumes: `UserPrincipal` (Task 10), `TranscriptionRepository` e `TranscriptionUtteranceRepository` (Task 2), `TestTokens` (Task 10), `Fixtures` (Task 2).
- Produces:
  - `GET /transcriptions?from&to&src&destination&peer&status&page&size` → `PageResponse<TranscriptionSummary>`
  - `GET /transcriptions/{id}` → `TranscriptionDetail`, ou 404.
  - `GET /transcriptions/by-cdr/{uniqueId}` → `List<TranscriptionDetail>`.
  - `POST /transcriptions/{id}/retry` → 204; 404 se a transcrição não existe ou é de outra empresa; 409 se ela não está `FAILED`.

- [ ] **Step 1: Escrever o teste que falha**

`src/test/kotlin/br/com/vipsolutions/vipia/transcription/api/TranscriptionControllerTest.kt`:
```kotlin
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.api.TranscriptionControllerTest"`
Expected: FAIL (rota inexistente retorna 404 nos testes de listagem).

- [ ] **Step 3: Implementar**

`common/PageResponse.kt`:
```kotlin
package br.com.vipsolutions.vipia.common

data class PageResponse<T>(
    val content: List<T>,
    val page: Int,
    val size: Int,
    val totalElements: Long,
)
```

`transcription/api/dto/TranscriptionDtos.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription.api.dto

import br.com.vipsolutions.vipia.transcription.TranscriptionEntity
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import br.com.vipsolutions.vipia.transcription.TranscriptionUtteranceEntity
import java.time.Instant

data class TranscriptionFilter(
    val companyId: String,
    val from: Instant?,
    val to: Instant?,
    val src: String?,
    val destination: String?,
    val peer: String?,
    val status: TranscriptionStatus?,
    val page: Int,
    val size: Int,
)

data class TranscriptionSummary(
    val id: Long,
    val cdrUniqueId: String,
    val recordName: String,
    val status: TranscriptionStatus,
    val callStartTime: Instant,
    val src: String,
    val destination: String,
    val peer: String,
    val disposition: String,
    val callDuration: Int,
    val audioSeconds: Double?,
    val attempts: Int,
    val lastError: String?,
) {
    constructor(entity: TranscriptionEntity) : this(
        id = requireNotNull(entity.id),
        cdrUniqueId = entity.cdrUniqueId,
        recordName = entity.recordName,
        status = entity.status,
        callStartTime = entity.callStartTime,
        src = entity.src,
        destination = entity.destination,
        peer = entity.peer,
        disposition = entity.disposition,
        callDuration = entity.callDuration,
        audioSeconds = entity.audioSeconds,
        attempts = entity.attempts,
        lastError = entity.lastError,
    )
}

data class UtteranceResponse(
    val speaker: Int,
    val startSeconds: Double,
    val endSeconds: Double,
    val content: String,
    val confidence: Double?,
) {
    constructor(entity: TranscriptionUtteranceEntity) : this(
        speaker = entity.speaker,
        startSeconds = entity.startSeconds,
        endSeconds = entity.endSeconds,
        content = entity.content,
        confidence = entity.confidence,
    )
}

data class TranscriptionDetail(
    val summary: TranscriptionSummary,
    val transcript: String?,
    val language: String?,
    val model: String?,
    val utterances: List<UtteranceResponse>,
)
```

`transcription/api/TranscriptionQueryRepository.kt`:
```kotlin
package br.com.vipsolutions.vipia.transcription.api

import br.com.vipsolutions.vipia.common.PageResponse
import br.com.vipsolutions.vipia.transcription.TranscriptionStatus
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionFilter
import br.com.vipsolutions.vipia.transcription.api.dto.TranscriptionSummary
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp

/** Listagem com filtros opcionais: SQL montado à mão porque os filtros são dinâmicos. */
@Repository
class TranscriptionQueryRepository(private val jdbc: NamedParameterJdbcTemplate) {

    private val rowMapper = RowMapper { rs, _ ->
        TranscriptionSummary(
            id = rs.getLong("id"),
            cdrUniqueId = rs.getString("cdr_unique_id"),
            recordName = rs.getString("record_name"),
            status = TranscriptionStatus.valueOf(rs.getString("status")),
            callStartTime = rs.getTimestamp("call_start_time").toInstant(),
            src = rs.getString("src"),
            destination = rs.getString("destination"),
            peer = rs.getString("peer"),
            disposition = rs.getString("disposition"),
            callDuration = rs.getInt("call_duration"),
            audioSeconds = rs.getDouble("audio_seconds").takeUnless { rs.wasNull() },
            attempts = rs.getInt("attempts"),
            lastError = rs.getString("last_error"),
        )
    }

    fun search(filter: TranscriptionFilter): PageResponse<TranscriptionSummary> {
        val params = MapSqlParameterSource("companyId", filter.companyId)
        val where = mutableListOf("company_id = :companyId")
        filter.from?.let { where += "call_start_time >= :from"; params.addValue("from", Timestamp.from(it)) }
        filter.to?.let { where += "call_start_time < :to"; params.addValue("to", Timestamp.from(it)) }
        filter.src?.let { where += "src = :src"; params.addValue("src", it) }
        filter.destination?.let { where += "destination = :destination"; params.addValue("destination", it) }
        filter.peer?.let { where += "peer = :peer"; params.addValue("peer", it) }
        filter.status?.let { where += "status = :status"; params.addValue("status", it.name) }
        val whereClause = where.joinToString(" AND ")

        val total = jdbc.queryForObject("SELECT COUNT(*) FROM transcription WHERE $whereClause", params, Long::class.java) ?: 0L
        params.addValue("limit", filter.size).addValue("offset", filter.page.toLong() * filter.size)
        val content = jdbc.query(
            """SELECT id, cdr_unique_id, record_name, status, call_start_time, src, destination, peer, disposition,
                      call_duration, audio_seconds, attempts, last_error
               FROM transcription WHERE $whereClause
               ORDER BY call_start_time DESC, id DESC
               LIMIT :limit OFFSET :offset""",
            params,
            rowMapper,
        )
        return PageResponse(content, filter.page, filter.size, total)
    }
}
```

`transcription/api/TranscriptionApiService.kt`:
```kotlin
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
```

`transcription/api/TranscriptionController.kt`:
```kotlin
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
```

- [ ] **Step 4: Rodar e ver passar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.transcription.api.TranscriptionControllerTest"` and then `./tests.sh`
Expected: PASS.

Se `from`/`to` não converterem de String para `Instant`, use `@DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)` nesses parâmetros. Não mude o formato aceito (ISO-8601 com `Z`).

Atenção: uniqueId tem ponto (`1772633126.1880`). Se o Spring cortar o sufixo depois do ponto em `/by-cdr/{uniqueId}`, troque o mapeamento para `@GetMapping("/by-cdr/{uniqueId:.+}")`.

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: API REST de transcricoes"
```

---

### Task 12: Relatório de consumo para o financeiro

**Files:**
- Create: `usage/api/dto/UsageDtos.kt`, `usage/api/UsageReportRepository.kt`, `usage/api/UsageController.kt`
- Test: `usage/api/UsageControllerTest.kt`

**Interfaces:**
- Consumes: `UsageRecordRepository`, `UsageType` e `UsageProvider` (Task 2); `TestTokens` (Task 10).
- Produces: `GET /internal/usage?companyId&from&to` → `UsageReport(companyId, from, to, items: List<UsageSummary>)`. Exige `ROLE_SAAS_FINANCE`; o período é `[from, to)`.

- [ ] **Step 1: Escrever o teste que falha**

`src/test/kotlin/br/com/vipsolutions/vipia/usage/api/UsageControllerTest.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage.api

import br.com.vipsolutions.vipia.support.IntegrationTest
import br.com.vipsolutions.vipia.support.TestTokens
import br.com.vipsolutions.vipia.usage.UsageProvider
import br.com.vipsolutions.vipia.usage.UsageRecordEntity
import br.com.vipsolutions.vipia.usage.UsageRecordRepository
import br.com.vipsolutions.vipia.usage.UsageType
import org.hamcrest.Matchers.hasSize
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.time.Instant

class UsageControllerTest : IntegrationTest() {

    @Autowired lateinit var usageRepository: UsageRecordRepository

    private fun usage(companyId: String, seconds: Double, at: String) = usageRepository.save(
        UsageRecordEntity(
            companyId = companyId,
            usageType = UsageType.TRANSCRIPTION,
            referenceId = 1,
            provider = UsageProvider.DEEPGRAM,
            model = "nova-3",
            audioSeconds = seconds,
            createdAt = Instant.parse(at),
        )
    )

    private fun request() = get("/internal/usage")
        .param("companyId", "100023")
        .param("from", "2026-10-01T00:00:00Z")
        .param("to", "2026-11-01T00:00:00Z")

    @Test
    fun `financeiro da plataforma ve o consumo agregado do periodo`() {
        usage("100023", 60.0, "2026-10-05T10:00:00Z")
        usage("100023", 30.5, "2026-10-20T10:00:00Z")
        usage("100023", 99.0, "2026-11-01T00:00:00Z") // fora: to é exclusivo
        usage("999999", 10.0, "2026-10-05T10:00:00Z") // outra empresa

        mockMvc.perform(request().header("Authorization", TestTokens.bearer(roles = listOf("ROLE_SAAS_FINANCE"))))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.companyId").value("100023"))
            .andExpect(jsonPath("$.items", hasSize<Any>(1)))
            .andExpect(jsonPath("$.items[0].provider").value("DEEPGRAM"))
            .andExpect(jsonPath("$.items[0].usageType").value("TRANSCRIPTION"))
            .andExpect(jsonPath("$.items[0].model").value("nova-3"))
            .andExpect(jsonPath("$.items[0].records").value(2))
            .andExpect(jsonPath("$.items[0].audioSeconds").value(90.5))
    }

    @Test
    fun `usuario de empresa nao acessa o relatorio`() {
        mockMvc.perform(request().header("Authorization", TestTokens.bearer(roles = listOf("ROLE_COMPANY_ADMIN"))))
            .andExpect(status().isForbidden)
    }

    @Test
    fun `sem token retorna 401`() {
        mockMvc.perform(request()).andExpect(status().isUnauthorized)
    }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.usage.api.UsageControllerTest"`
Expected: FAIL (com token de financeiro, a rota inexistente retorna 404).

- [ ] **Step 3: Implementar**

`usage/api/dto/UsageDtos.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage.api.dto

import br.com.vipsolutions.vipia.usage.UsageProvider
import br.com.vipsolutions.vipia.usage.UsageType
import java.math.BigDecimal
import java.time.Instant

data class UsageSummary(
    val provider: UsageProvider,
    val usageType: UsageType,
    val model: String?,
    val records: Long,
    val audioSeconds: Double,
    val inputTokens: Long,
    val outputTokens: Long,
    /** NULL quando o provedor não informa custo (Deepgram): o financeiro aplica o preço por minuto. */
    val cost: BigDecimal?,
)

data class UsageReport(
    val companyId: String,
    val from: Instant,
    val to: Instant,
    val items: List<UsageSummary>,
)
```

`usage/api/UsageReportRepository.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage.api

import br.com.vipsolutions.vipia.usage.UsageProvider
import br.com.vipsolutions.vipia.usage.UsageType
import br.com.vipsolutions.vipia.usage.api.dto.UsageSummary
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant

@Repository
class UsageReportRepository(private val jdbc: NamedParameterJdbcTemplate) {

    fun summarize(companyId: String, from: Instant, to: Instant): List<UsageSummary> = jdbc.query(
        """SELECT provider, usage_type, model,
                  COUNT(*) AS records,
                  COALESCE(SUM(audio_seconds), 0) AS audio_seconds,
                  COALESCE(SUM(input_tokens), 0) AS input_tokens,
                  COALESCE(SUM(output_tokens), 0) AS output_tokens,
                  SUM(cost) AS cost
           FROM usage_record
           WHERE company_id = :companyId AND created_at >= :from AND created_at < :to
           GROUP BY provider, usage_type, model
           ORDER BY provider, usage_type, model""",
        MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("from", Timestamp.from(from))
            .addValue("to", Timestamp.from(to)),
    ) { rs, _ ->
        UsageSummary(
            provider = UsageProvider.valueOf(rs.getString("provider")),
            usageType = UsageType.valueOf(rs.getString("usage_type")),
            model = rs.getString("model"),
            records = rs.getLong("records"),
            audioSeconds = rs.getDouble("audio_seconds"),
            inputTokens = rs.getLong("input_tokens"),
            outputTokens = rs.getLong("output_tokens"),
            cost = rs.getBigDecimal("cost"),
        )
    }
}
```

`usage/api/UsageController.kt`:
```kotlin
package br.com.vipsolutions.vipia.usage.api

import br.com.vipsolutions.vipia.usage.api.dto.UsageReport
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

/** Consumido pelo relatório financeiro (manager). Roles são cumulativas: SAAS_ADMIN também tem SAAS_FINANCE. */
@RestController
@RequestMapping("/internal/usage")
class UsageController(private val repository: UsageReportRepository) {

    @GetMapping
    @PreAuthorize("hasAuthority('ROLE_SAAS_FINANCE')")
    fun report(
        @RequestParam companyId: String,
        @RequestParam from: Instant,
        @RequestParam to: Instant,
    ): UsageReport = UsageReport(companyId, from, to, repository.summarize(companyId, from, to))
}
```

- [ ] **Step 4: Rodar e ver passar**

Run: `./gradlew test --tests "br.com.vipsolutions.vipia.usage.api.UsageControllerTest"` and then `./tests.sh`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: relatorio de consumo para o financeiro"
```

---

### Task 13: Verificação final e sincronização da documentação

**Files:**
- Modify: `vip-ia/AGENTS.md`, `vip-ia/MANIFESTO.md`, só se a implementação divergiu do que eles descrevem.

- [ ] **Step 1: Build completo**

Run: `./build.sh`
Expected: `BUILD SUCCESSFUL`, com todos os testes passando e o jar em `build/libs/vip-ia-0.0.1-SNAPSHOT.jar`.

- [ ] **Step 2: Conferir a documentação contra o código**

Confira, item por item:
- As variáveis da tabela de `.env` do `AGENTS.md` batem com `.env.example`, incluindo `AWS_S3_REGION` e `DEPLOY_HOST`.
- O contrato da seção 5 do `MANIFESTO.md` bate com `src/test/resources/kafka-contracts/ia-cdr-message.json`.
- As rotas da seção 7 do `MANIFESTO.md` que são da fase 1 existem.

Corrija o que divergir.

- [ ] **Step 3: Commit**

```bash
git add .
git commit -m "docs: sincroniza AGENTS e MANIFESTO com a implementacao da fase 1"
```

- [ ] **Step 4: Revisão e integração**

Siga a Definition of Done do `AGENTS.md`: `superpowers:requesting-code-review` → `superpowers:receiving-code-review` → `superpowers:finishing-a-development-branch`.

---

## Fora deste plano (dependências externas)

- **vip-pabx-manager**:
  - criar a flag de módulo de IA por empresa;
  - criar o tópico `KAFKA_MANAGER_IA_CDR_TOPIC`;
  - criar o produtor da `IaCdrMessage`, que monta `recordNames` a partir de `channelInteractions[].mixedRecord`;
  - copiar o golden file `ia-cdr-message.json` para os testes de contrato do manager.

  Isso merece um plano próprio no repositório do manager.
- **Infra**: criar o banco `vip_ia` e rodar as migrations (`docs/db-migrations-log.md`), criar o usuário Kafka do vip-ia com ACL de leitura no tópico novo, liberar `s3:GetObject` no bucket de gravações para a credencial do vip-ia e instalar a unit systemd.
- **Fase 2 (agentes/OpenRouter)**: plano separado, depois que a fase 1 estiver em produção.
