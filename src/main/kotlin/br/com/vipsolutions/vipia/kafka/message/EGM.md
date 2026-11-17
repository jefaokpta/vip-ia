# kafka/message

## Overview
Contrato da mensagem consumida do Kafka: a CDR republicada pelo vip-pabx-manager no tópico `KAFKA_MANAGER_IA_CDR_TOPIC`, apenas para empresas com o módulo de IA.

## Interface
- `IaCdrMessage(companyId: String, uniqueId: String, startTime: Long, duration: Int, billableSeconds: Int, src: String, destination: String, peer: String, disposition: String, userfield: String, recordNames: List<String> = emptyList())`

## Invariants
- O formato é espelhado no manager; o contrato de referência fica em `src/test/resources/kafka-contracts/ia-cdr-message.json`. Mudanças de campo exigem atualizar os dois lados.
- `recordNames` são os `mixedRecord` (sem extensão) das `channelInteractions` da CDR; o `callRecords` da CDR do worker vem sempre vazio e não é usado.
- `startTime` é epoch em milissegundos (convertido com `Instant.ofEpochMilli` em `TranscriptionTaskService`).

## Side Effects
- None

## Verification
- Run: `./tests.sh --tests 'br.com.vipsolutions.vipia.kafka.*'` (na raiz `vip-ia/`)
