# Log de migrations — vip-ia

Scripts a rodar manualmente no MySQL de produção do vip-ia, na ordem.

## 2026-10-15 — Fase 1 (transcrição)

Pré-requisito: `CREATE DATABASE vip_ia CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;`

1. `src/main/resources/db/migration/create_transcription.sql`
2. `src/main/resources/db/migration/create_transcription_utterance.sql`
3. `src/main/resources/db/migration/create_usage_record.sql`
