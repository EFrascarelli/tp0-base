#!/bin/bash

OUT_FILE="$1"
NUM_CLIENTS="$2"

echo "Nombre del archivo de salida: $OUT_FILE"
echo "Cantidad de clientes: $NUM_CLIENTS"

# Invoca el script de Python, pasándole nombre de archivo y cantidad
python3 generador_compose.py "$OUT_FILE" "$NUM_CLIENTS"