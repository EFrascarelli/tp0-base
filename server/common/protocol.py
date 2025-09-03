from datetime import datetime
import json
import logging
from common.utils import Bet

def _recv_n_bytes(sock, n: int) -> bytes:
    """Lee exactamente n bytes del socket (reintentando hasta completar)."""
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:  # peer cerró antes de tiempo → short read
            raise ConnectionError("short read: peer closed connection")
        data.extend(chunk)
    return bytes(data)

def get_bet(client_sock, max_len: int = 16 * 1024):
    prev_timeout = None
    try:
        try:
            prev_timeout = client_sock.gettimeout()
        except Exception:
            prev_timeout = None
        client_sock.settimeout(2.0)

        # Header (4 bytes) → entero big-endian
        header = _recv_n_bytes(client_sock, 4)
        length = int.from_bytes(header, byteorder="big", signed=False)

        if length <= 0 or length > max_len:
            logging.error(f"action: receive_message | result: fail | step: too_large_or_invalid | length: {length}")
            raise ValueError(f"invalid frame length: {length}")

        # Body (len bytes)
        payload = _recv_n_bytes(client_sock, length)

        # Decode + JSON
        try:
            line = payload.decode("utf-8")
        except UnicodeDecodeError as e:
            logging.error(f"action: receive_message | result: fail | step: utf8_decode | error: {e}")
            raise

        bet_obj = parse_bet_line(line)
        logging.info(f"action: receive_message | result: success | step: framed_read | length: {length}")
        return bet_obj

    finally:
        # restaurar timeout original
        try:
            client_sock.settimeout(prev_timeout)
        except Exception:
            pass

def send_bet_ack_ok(client_sock, dni: str, numero: int):
    # Formato textual: ACK|OK|<dni>|<numero>
    line = f"ACK|OK|{_escape(dni)}|{numero}"
    payload = line.encode("utf-8")
    header = len(payload).to_bytes(4, byteorder="big")
    client_sock.sendall(header + payload)

def send_bet_ack_fail(client_sock, code: str, reason: str):
    # Formato textual: ACK|FAIL|<code>|<reason>
    line = f"ACK|FAIL|{code}|{_escape(reason)}"
    payload = line.encode("utf-8")
    header = len(payload).to_bytes(4, byteorder="big")
    client_sock.sendall(header + payload)

def _escape(s: str) -> str:
    return s.replace('\\', '\\\\').replace('|', '\\|').replace('\n', '\\n')

def _unescape(s: str) -> str:
    out = []
    i = 0
    while i < len(s):
        if s[i] == '\\' and i + 1 < len(s):
            nxt = s[i+1]
            if nxt == '\\':
                out.append('\\'); i += 2; continue
            if nxt == '|':
                out.append('|'); i += 2; continue
            if nxt == 'n':
                out.append('\n'); i += 2; continue
        out.append(s[i]); i += 1
    return ''.join(out)

def parse_bet_line(line: str) -> "Bet":
    # Formato textual: BET|dni|numero|nombre|apellido|nacimiento|agencia_id
    parts = line.split('|')
    if len(parts) != 7 or parts[0] != 'BET':
        raise ValueError(f'bad bet line: {line!r}')
    
    dni        = _unescape(parts[1])
    numero_str = parts[2]
    nombre     = _unescape(parts[3])
    apellido   = _unescape(parts[4])
    nacimiento = _unescape(parts[5])
    agencia_id = parts[6]

    try:
        numero = int(numero_str)
    except ValueError:
        raise ValueError(f'bad numero: {numero_str!r}')
    try:
        agid = int(agencia_id)
    except ValueError:
        raise ValueError(f'bad agencia_id: {agencia_id!r}')

    # Crear un Bet directamente
    return Bet(
        agency=agid,
        first_name=nombre,
        last_name=apellido,
        document=dni,
        birthdate=nacimiento,
        number=numero,
    )
