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
