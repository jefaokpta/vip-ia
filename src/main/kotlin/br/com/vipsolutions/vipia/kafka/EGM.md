# kafka

## Overview
Entrada assíncrona do serviço: consome as CDRs de IA do Kafka (SASL/SCRAM) e as transforma em tarefas de transcrição persistidas. A transcrição em si não acontece aqui; roda depois no `TranscriptionProcessor`.

## Interface
- `IaCdrListener.onMessage(payload: String)`: `@KafkaListener` com `containerFactory = "iaCdrListenerContainerFactory"`, tópico `${spring.kafka.consumer.ia-cdr-topic}`, grupo `${spring.kafka.consumer.group-id}`, `autoStartup = ${vip-ia.kafka.listener.auto-startup:true}`. Chama `TranscriptionTaskService.enqueue`.
- `IaCdrMessageParser.parse(payload: String): IaCdrMessage?`
- `KafkaConsumerConfig`: beans `iaCdrConsumerFactory()` (`ConsumerFactory<String, String>`) e `iaCdrListenerContainerFactory()`.
- `KafkaSecurityConfig.saslConfigs(): Map<String, Any>`: `SASL_PLAINTEXT` + `SCRAM-SHA-512` com JAAS montado a partir de `spring.kafka.sasl.username/password`.
- `IaCdrErrorHandling` (`object`): `errorHandler(): DefaultErrorHandler`, `backOffFor(error: Exception): BackOff?`.
- Subdiretório: [`message`](./message/EGM.md) - contrato `IaCdrMessage`.

## Invariants
- O valor é desserializado como `String` e convertido pelo `IaCdrMessageParser`: JSON inválido ou mensagem com `companyId`/`uniqueId` em branco é logada e descartada (`null`), sem travar a partição.
- O listener só persiste tarefas (rápido), para o offset ser confirmado logo e gravações ainda ausentes no S3 não bloquearem o consumo.
- Erros de indisponibilidade do banco (`TransientDataAccessException`, `DataAccessResourceFailureException`, `RecoverableDataAccessException` em qualquer nível da cadeia de causas) são retentados para sempre com `ExponentialBackOff(1_000, 2.0)` e `maxInterval = 60_000`, segurando a partição; os demais erros usam o backoff padrão do `DefaultErrorHandler`.
- `auto.offset.reset = earliest`.
- `KafkaSecurityConfig` falha no boot (`require`) se `KAFKA_SASL_USERNAME` ou `KAFKA_SASL_PASSWORD` estiverem vazios; como as factories usam mapas explícitos, propriedades de segurança `spring.kafka.*` não são aplicadas automaticamente.
- Reentregas são seguras: a idempotência é garantida em `TranscriptionTaskService` pela chave única `(company_id, cdr_unique_id, record_name)`.

## Side Effects
- Conexão de consumidor Kafka (`spring.kafka.bootstrap-servers`, `client-id: vip-ia-consumer`) e commit de offsets.
- Inserções na tabela `transcription` (via `TranscriptionTaskService`).
- Logs de aviso para mensagens descartadas.

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.kafka.*'` (na raiz `vip-ia/`)
