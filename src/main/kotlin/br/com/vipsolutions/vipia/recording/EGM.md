# recording

## Overview
Porta de acesso às gravações de áudio das chamadas. Em produção a implementação lê do S3 (onde o vip-worker sobe os arquivos); nos testes é substituída por um fake em memória.

## Interface
- `RecordingStorage` (interface)
  - `download(companyId: String, recordName: String): ByteArray`: lança `RecordingNotFoundException` se a gravação ainda não existe.
- `S3RecordingStorage` (`@Component`): implementação usando o bucket `aws.s3.recordings.bucket`.
- `RecordingNotFoundException(key: String)`
- `AwsS3Config.s3Client(): S3Client`: cliente na região `aws.s3.region`.

## Invariants
- Chave do objeto: `recordings/{companyId}/{recordName}.mp3` (`S3RecordingStorage.download`).
- `NoSuchKeyException` vira `RecordingNotFoundException`, que o `TranscriptionProcessor` trata como falha retentável (o worker pode subir o áudio depois da CDR).
- Credenciais AWS vêm da cadeia padrão do SDK (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`); não há credencial no código.

## Side Effects
- Leitura (`GetObject`) no bucket S3 de gravações; o áudio inteiro é carregado em memória.

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.recording.*'` (na raiz `vip-ia/`)
