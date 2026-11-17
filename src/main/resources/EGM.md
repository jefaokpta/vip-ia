# resources

## Overview
Configuração de runtime do vip-ia (`application.yml`). O DDL versionado fica em `db/migration`.

## Interface
- `application.yml`: `server.port` (`SERVER_PORT`, padrão 8083), `spring.datasource` (`DB_*`), `spring.kafka` (SASL, `KAFKA_GROUP_ID`, `ia-cdr-topic` = `KAFKA_MANAGER_IA_CDR_TOPIC`), `aws.s3` (`AWS_S3_REGION`, `AWS_S3_RECORDINGS_BUCKET`), `deepgram` (`DEEPGRAM_API_KEY`, `timeout-seconds: 300`), `auth.jwt.secret` (`AUTH_JWT_SECRET`).
- `vip-ia.transcription.*`: `poll-interval` PT10S, `batch-size` 20, `max-concurrency` 4, `max-attempts` 6, `retry-base-delay` PT1M, `language` pt-BR, `stuck-timeout` PT10M.
- `vip-ia.kafka.listener.auto-startup` e `vip-ia.scheduling.enabled` (ambos `true`; os testes desligam).
- DDL: [db/migration/EGM.md](./db/migration/EGM.md).

## Invariants
- `stuck-timeout` precisa ser maior que `deepgram.timeout-seconds` mais o tempo de download do S3.
- `spring.sql.init.mode: never`: o schema nunca é aplicado no boot.
- Segredos só via variáveis de ambiente.
- O Actuator expõe só `health` e `prometheus`, com discovery desabilitado e `health.show-details: never`.

## Side Effects
- None em si mesmo. Os efeitos são das conexões configuradas (MySQL, Kafka, S3, Deepgram).

## Verification
- Run: `./tests.sh`
