#!/bin/bash

# --- Config inicial ---
OUTCOME="fail"
MSG="hola-echo"
SERVER_SERVICE="server"
SERVER_PORT=12345   # ajustalo si tu config.ini usa otro puerto
COMPOSE_FILE="docker-compose-dev.yaml"

# --- Descubrir red de docker-compose ---
SERVER_CID=$(docker compose -f "$COMPOSE_FILE" ps -q $SERVER_SERVICE 2>/dev/null)
if [ -z "$SERVER_CID" ]; then
  echo "action: test_echo_server | result: fail | error: server container not found"
  exit 1
fi

NETWORK_NAME=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{printf "%s\n" $k}}{{end}}' "$SERVER_CID" | head -n1)

if [ -z "$NETWORK_NAME" ]; then
  echo "action: test_echo_server | result: fail | error: network not found"
  exit 1
fi

# --- Ejecutar prueba con busybox+nc ---
REPLY=$(docker run --rm --network "$NETWORK_NAME" busybox sh -c \
  "echo '$MSG' | nc -w 2 $SERVER_SERVICE $SERVER_PORT")

# --- Comparar ---
if [ "$REPLY" = "$MSG" ]; then
  OUTCOME="success"
fi

echo "action: test_echo_server | result: $OUTCOME"

if [ "$OUTCOME" = "success" ]; then
  exit 0
else
  exit 1
fi