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

sh gradlew bootRun
