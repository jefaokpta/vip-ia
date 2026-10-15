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
