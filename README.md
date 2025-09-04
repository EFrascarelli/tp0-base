# Descripción General

Este trabajo práctico consiste en el desarrollo de un sistema distribuido basado en Docker, compuesto por un servidor central y múltiples clientes que simulan agencias de la Lotería Nacional.

El proyecto se divide en tres partes principales:
	1.	Introducción a Docker: scripts y configuración básica para levantar los contenedores.
	2.	Repaso de Comunicaciones: implementación de un protocolo de comunicación entre clientes y servidor, manejo de apuestas, confirmaciones, loterías y consultas de ganadores.
	3.	Repaso de Concurrencia: incorporación de multithreading en el servidor para procesar múltiples conexiones en paralelo de forma segura.

La ejecución completa del sistema (servidor + clientes) se realiza con:

```
make docker-compose-up
```
Esto levanta los contenedores definidos en el docker-compose.yaml, inyecta las configuraciones necesarias y permite correr las pruebas automáticas provistas.

# Índice

- [Descripción General](#descripción-general)
- [Parte 1: Introducción a Docker](#parte-1-introducción-a-docker)
  - [Ejercicio 1: Script generar-compose.sh](#ejercicio-1-script-generar-composesh)
  - [Ejercicio 2: Configuración inyectada con volúmenes](#ejercicio-2-configuración-inyectada-con-volúmenes)
  - [Ejercicio 3: Script validar-echo-server.sh](#ejercicio-3-script-validar-echo-serversh)
  - [Ejercicio 4: Finalización graceful con SIGTERM](#ejercicio-4-finalización-graceful-con-sigterm)


## Parte 1 – Introducción a Docker

### Ejercicio 1 – Generador de Docker Compose
Para facilitar la creación de entornos con múltiples clientes, se implementó el script `generar-compose.sh`, ubicado en la raíz del proyecto.  

Este script recibe dos parámetros: el nombre del archivo de salida y la cantidad de clientes. Internamente invoca a un [generador](generador_compose.py) en Python, el cual construye un archivo `docker-compose` con los servicios **server** y `clientN` según corresponda.

**Ejemplo de uso**:
```bash
./generar-compose.sh docker-compose-dev.yaml 5
```

### Decisiones:

•	**Validador del generador**: el script rechaza nombres de salida sin .yaml y cantidades no numéricas para evitar archivos basura.

•	Se implementó un **script generador** en Bash que invoca a un **subscript en Python**.


**Resultado**:  
Se genera un archivo `docker-compose-dev.yaml` con un servidor y 5 clientes (`client1` … `client5`), cada uno configurado con su dataset, variables de entorno y red común.

---

### Ejercicio 2 – Configuración inyectada
Se modificaron cliente y servidor para que sus configuraciones no requieran reconstruir imágenes de Docker.  

- **Servidor**: configuración en `config.ini`, montada como volumen.  
- **Cliente**: configuración en `config.yaml`, también montada como volumen.  

De esta forma, cualquier cambio en los parámetros (ej. logging level, batch size) se refleja inmediatamente al reiniciar los contenedores.

### Decisiones
•	Evité .env cuando la clave ya existe en archivo de config, para no tener fuentes de config en conflicto.


---

### Ejercicio 3 – Validación con Echo Server
Se desarrolló el script `validar-echo-server.sh`, que permite verificar el correcto funcionamiento del servidor en modo echo utilizando `netcat`.  
El script corre dentro de la red de Docker (sin exponer puertos al host).  

**Ejemplo de uso**:
```bash
./validar-echo-server.sh
```

**Salida esperada**:
```
action: test_echo_server | result: success
```

En caso de error:
```
action: test_echo_server | result: fail
```

### Decisiones
•	El script corre nc desde un contenedor en la misma red (no desde el host) para testear exactamente el escenario de runtime.

---

### Ejercicio 4 – Terminación Graceful
Se implementó el manejo de la signal `SIGTERM` en servidor y cliente, logrando un **cierre ordenado**:

- Cierre de sockets.  
- Liberación de threads y file descriptors.  
- Logs explícitos de cierre de cada recurso.  

Esto asegura que al ejecutar:
```bash
docker compose down -t 5
```
los procesos se detengan correctamente sin dejar recursos abiertos.

### Decisiones
•	Deadlines/Timeouts al leer/escribir sockets para evitar quedar bloqueado si llega SIGTERM en medio de IO.

---

# Parte 2: Repaso de Comunicaciones

En esta segunda parte del trabajo práctico se plantea un caso de uso denominado **Lotería Nacional**.  
El objetivo es diseñar e implementar un protocolo de comunicación entre clientes (agencias) y un servidor (central), garantizando la correcta serialización, separación de responsabilidades y manejo robusto de sockets.  
Los ejercicios incluyen desde el envío de apuestas individuales hasta el manejo de lotes (batch) y la notificación de fin de envío para proceder con el sorteo.

## Ejercicio 5
Protocolo de Mensajería
	•	Se definió un protocolo textual con framing binario (4 bytes big-endian para indicar longitud del mensaje).
	
•	Los mensajes individuales siguen el formato:
```
BET|<dni>|<numero>|<nombre>|<apellido>|<nacimiento>|<agencia_id>\n
```
•	El servidor responde con un ACK textual:
```
ACK|OK|<dni>|<numero>

ACK|FAIL|<codigo>|<razon>
```
•	Para evitar ambigüedades, se implementaron funciones de escape y unescape de caracteres especiales (|, \n, \).

### Serialización

•	No se usaron librerías JSON (prohibido por consigna).

•	Se serializó manualmente a cadenas delimitadas por |.

•	Se cuidó el manejo de short read y short write en sockets con funciones readExact y writeFull.

### Decisiones tomadas
•	Mantener un framing binario (4 bytes) + payload textual para combinar robustez y simplicidad.

•	Incluir escape/unescape para que los datos del usuario no rompan el protocolo.

•	Implementar ACKs explícitos para que los clientes puedan saber si la apuesta se registró o falló.
