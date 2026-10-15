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
