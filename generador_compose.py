import sys 

def get_logging_level(side):
    if side == 'client':
        with open('client/config.yaml', 'r') as f:
            for line in f:
                if line.startswith("log:"):
                    next_line = next(f).strip()
                    if next_line.startswith("level:"):
                        return next_line.split(":")[1].strip().strip('"')
    elif side == 'server':
        with open('server/config.ini', 'r') as f:
            for line in f:
                if line.startswith("LOGGING_LEVEL"):
                    return line.split("=")[1].strip()
    return "INFO"

def main():
    if len(sys.argv) != 3:
        print("Uso: python3 generador_compose.py <nombre_archivo_salida> <cantidad_clientes>")
        sys.exit(1)

    if not sys.argv[1].endswith(".yaml"):
        print("El nombre del archivo de salida debe terminar con .yml")
        sys.exit(1)

    if not sys.argv[2].isdigit():
        print("La cantidad de clientes debe ser un número entero.")
        sys.exit(1)



    out_file = sys.argv[1]
    num_clients = int(sys.argv[2])

    with open(out_file, 'w') as f:
        f.write("name: tp0\n")
        f.write("services:\n")
        f.write("  server:\n")
        f.write("    container_name: server\n")
        f.write("    image: server:latest\n")
        f.write("    entrypoint: python3 /main.py\n")
        f.write("    volumes:\n")
        f.write("      - ./server/config.ini:/config.ini\n")
        f.write("    environment:\n")
        f.write("      - PYTHONUNBUFFERED=1\n")
        f.write(f"      - LOGGING_LEVEL={get_logging_level('server')}\n")
        f.write("    networks:\n")
        f.write("      - testing_net\n")
        f.write("\n")
        for i in range(num_clients):
            f.write(f"  client{i+1}:\n")
            f.write(f"    container_name: client{i+1}\n")
            f.write(f"    image: client:latest\n")
            f.write(f"    entrypoint: /client\n")
            f.write(f"    volumes:\n")
            f.write(f"      - ./client/config.yaml:/config.yaml\n")
            f.write(f"      - ./.data:/data:ro\n") 
            f.write(f"    environment:\n")
            f.write(f"      - CLI_ID={i+1}\n")
            f.write(f"      - CLI_LOG_LEVEL={get_logging_level('client')}\n")
            f.write(f"      - REQUIRED_AGENCIES={num_clients}\n")
            f.write(f"    networks:\n")
            f.write(f"      - testing_net\n")
            f.write(f"    depends_on:\n")
            f.write(f"      - server\n")
            f.write("\n")

        f.write("networks:\n")
        f.write("  testing_net:\n")
        f.write("    ipam:\n")
        f.write("      driver: default\n")
        f.write("      config:\n")
        f.write("        - subnet: 172.25.125.0/24\n")

    print(f"Archivo {out_file} generado con {num_clients} clientes.")

main()