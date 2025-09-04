# Parte 3: Repaso de Concurrencia

En esta sección se aborda el **Ejercicio 8**, cuyo objetivo es modificar el servidor para que acepte conexiones y procese mensajes en paralelo.

---

## Ejercicio 8: Concurrencia en el Servidor

### Descripción
El servidor fue extendido para soportar concurrencia mediante **multithreading**.  
Cada vez que un cliente se conecta, se crea un nuevo hilo dedicado a procesar sus mensajes, mientras el servidor principal continúa aceptando nuevas conexiones.

Esto asegura que múltiples clientes puedan enviar apuestas, notificar finalización y consultar ganadores al mismo tiempo, sin que las operaciones se bloqueen mutuamente.

### Mecanismos de sincronización
Dado que la persistencia de las apuestas (`store_bets`, `load_bets`) accede a recursos compartidos, se incorporaron mecanismos de **locks (threading.Lock)** para proteger secciones críticas.  
De esta forma, se evita la corrupción de datos o condiciones de carrera.

### Decisiones tomadas
- Se utilizó el módulo estándar `threading` de Python para el manejo de concurrencia.
- Se creó un archivo auxiliar `threading_tools.py` que encapsula las funciones de ejecución segura con locks, facilitando la reutilización y la limpieza del código.
- En la clase `Server`, cada conexión aceptada se delega a un hilo usando `threading.Thread(target=..., daemon=True)`.
- Se decidió mantener un lock global para operaciones críticas sobre la persistencia en lugar de múltiples locks, priorizando la simplicidad.

### Logs de ejemplo
**Éxito en ejecución concurrente:**
```text
server   | action: accept_connections | result: success | ip: 172.25.125.4
server   | action: receive_message | result: success | ip: 172.25.125.4 | msg_type: batch
server   | action: apuesta_recibida | result: success | cantidad: 10
server   | action: sorteo | result: success
```

**Error controlado en sección crítica:**
```text
server   | action: sorteo | step: eval_bet | result: fail | error: invalid birthdate format
```

### Ejecución
Para levantar el entorno concurrente:
```bash
make docker-compose-up
```

Esto iniciará servidor y clientes en paralelo, validando que el servidor pueda procesar múltiples conexiones de forma concurrente.
