from datetime import datetime
import logging

def _recv_n_bytes(sock, n: int) -> bytes:
    """Lee exactamente n bytes del socket (reintentando hasta completar)."""
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:  # peer cerró antes de tiempo → short read
            raise ConnectionError("short read: peer closed connection")
        data.extend(chunk)
    return bytes(data)

def _parse_bet_line(line: str) -> dict:
    parts = line.split('|')
    if len(parts) != 7 or parts[0] != 'BET':
        raise ValueError(f'bad bet line: {line!r}')
    document        = _unescape(parts[1])
    number_str = parts[2]
    first_name     = _unescape(parts[3])
    last_name   = _unescape(parts[4])
    birthdate = _unescape(parts[5])
    agency_id = parts[6]
    number = int(number_str)
    agid   = int(agency_id)
    return {
        "document": document,
        "number": number,
        "first_name": first_name,
        "last_name": last_name,
        "birthdate": birthdate,
        "agency": agid,
    }

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

        # Decode + protocolo textual (BET|... o BATCH|N + N líneas BET|...)
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as e:
            logging.error(f"action: receive_message | result: fail | step: utf8_decode | error: {e}")
            raise

        lines = text.splitlines()
        if not lines:
            raise ValueError("empty payload")

        head = lines[0].split('|')
        kind = head[0]

        # NOTIFY|DONE|<agency_id>
        if kind == 'NOTIFY' and len(head) == 3 and head[1] == 'DONE':
            agid = int(head[2])
            logging.info(f"action: receive_message | result: success | step: notify_done | agency: {agid}")
            return ("finish", agid)

        # WINQ|<agency_id>
        if kind == 'WINQ' and len(head) == 2:
            agid = int(head[1])
            logging.info(f"action: receive_message | result: success | step: winners_query | agency: {agid}")
            return ("winners_query", agid)

        if kind == 'BET':
            bet = _parse_bet_line(lines[0])
            logging.info(f"action: receive_message | result: success | step: framed_read | length: {length}")
            # devolvemos un “sobre” textual: (tipo, payload)
            return ("bet", bet)

        if kind == 'BATCH':
            if len(head) != 2:
                raise ValueError(f'bad batch header: {lines[0]!r}')
            n = int(head[1])
            if len(lines) - 1 != n:
                raise ValueError(f'batch count mismatch: header={n} lines={len(lines)-1}')
            items = []
            for i in range(1, len(lines)):
                items.append(_parse_bet_line(lines[i]))
            logging.info(f"action: receive_message | result: success | step: framed_read | length: {length}")
            return ("batch", items)

        raise ValueError(f'unknown message type: {kind!r}')

    finally:
        # restaurar timeout original
        try:
            client_sock.settimeout(prev_timeout)
        except Exception:
            pass

def send_bet_confirmation(client_sock, bet):
    """
    ACK individual textual: ACK|OK|<dni>|<numero>
    """
    try:
        dni = bet.get("document")
        numero = bet.get("number")
        line = f"ACK|OK|{_escape(dni)}|{int(numero)}"
        payload = line.encode("utf-8")
        header = len(payload).to_bytes(4, "big", signed=False)
        client_sock.sendall(header + payload)
        logging.info(f'action: send_ack | result: success | dni: {dni} | numero: {numero}')
    except Exception as e:
        logging.error(f'action: send_ack | result: fail | error: {e}')

def send_batch_ack_success(client_sock, count: int) -> None:
    try:
        line = f"ACKB|OK|{int(count)}"
        payload = line.encode("utf-8")
        header = len(payload).to_bytes(4, "big", signed=False)
        client_sock.sendall(header + payload)
        logging.info(f'action: send_ack | result: success | type: ack_batch | count: {count}')
    except Exception as e:
        logging.error(f'action: send_ack | result: fail | type: ack_batch | error: {e}')

def send_batch_ack_fail(client_sock, count: int, code: str, reason: str) -> None:
    try:
        line = f"ACKB|FAIL|{_escape(code)}|{_escape(reason)}"
        payload = line.encode("utf-8")
        header = len(payload).to_bytes(4, "big", signed=False)
        client_sock.sendall(header + payload)
        logging.info(f'action: send_ack | result: success | type: ack_batch | count: {count} | step: nack_sent')
    except Exception as e:
        logging.error(f'action: send_ack | result: fail | type: ack_batch | error: {e}')

def send_finish_ack(client_sock):
    try:
        line = "ACKN|OK\n"
        payload = line.encode("utf-8")
        header = len(payload).to_bytes(4, "big", signed=False)
        client_sock.sendall(header + payload)
        logging.info('action: send_ack | result: success | type: ack_notify_done')
    except Exception as e:
        logging.error(f'action: send_ack | result: fail | type: ack_notify_done | error: {e}')

def send_winners_ok(client_sock, winners):
    """
    Respuesta éxito a consulta de ganadores:
      WRES|OK|<count>|dni1,dni2,...
    (si count=0, se omite la 4ta parte)
    """
    try:
        count = len(winners)
        if count > 0:
            line = f"WRES|OK|{count}|{','.join(winners)}"
        else:
            line = f"WRES|OK|0"
        payload = line.encode("utf-8")
        header = len(payload).to_bytes(4, "big", signed=False)
        client_sock.sendall(header + payload)
        logging.info(f'action: send_winners | result: success | count: {count}')
    except Exception as e:
        logging.error(f'action: send_winners | result: fail | error: {e}')

def send_winners_fail(client_sock, code, reason):
    """
    Respuesta error a consulta de ganadores:
      WRES|FAIL|<code>|<reason>
    """
    try:
        line = f"WRES|FAIL|{_escape(code)}|{_escape(reason)}"
        payload = line.encode("utf-8")
        header = len(payload).to_bytes(4, "big", signed=False)
        client_sock.sendall(header + payload)
        logging.info(f'action: send_winners | result: success | step: nack_sent | code: {code}')
    except Exception as e:
        logging.error(f'action: send_winners | result: fail | error: {e}')

def _escape(s: str) -> str:
    return s.replace('\\', '\\\\').replace('|', '\\|').replace('\n', '\\n')

def _unescape(s: str) -> str:
    out = []
    i = 0
    while i < len(s):
        if s[i] == '\\' and i + 1 < len(s):
            nxt = s[i+1]
            if nxt == '\\': out.append('\\'); i += 2; continue
            if nxt == '|':  out.append('|');  i += 2; continue
            if nxt == 'n':  out.append('\n'); i += 2; continue
        out.append(s[i]); i += 1
    return ''.join(out)
