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
