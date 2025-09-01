#!/bin/bash

NETWORK_NAME=tp0_testing_net
SERVER_SERVICE="server"
SERVER_PORT=12345
MSG="Testing Message From SH"

# obtener IP del server en la red
SERVER_IP=$(docker network inspect $NETWORK_NAME \
  | awk -v srv="$SERVER_SERVICE" '
      /"Name":/ && index($0, srv) {getline; getline; getline; print $2}' \
  | tr -d '",/' )

# ejecutar prueba con busybox+nc
RESPONSE=$(echo "$MSG" | docker run --rm --network="$NETWORK_NAME" -i busybox sh -c "nc -w 2 server $SERVER_PORT" | tr -d '\r')

# comparar
if [ "$RESPONSE" = "$MSG" ]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
fi