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
