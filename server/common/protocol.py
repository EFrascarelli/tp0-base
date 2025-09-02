from datetime import datetime
import json
import logging

class BetObj:
    def __init__(self, d):
        self.document = d.get("dni")
        self.number = d.get("numero")
        self.first_name = d.get("nombre")
        self.last_name = d.get("apellido")
        self.birthdate = d.get("nacimiento")
        self.agency = d.get("agencia_id")

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
            text = payload.decode("utf-8")
        except UnicodeDecodeError as e:
            logging.error(f"action: receive_message | result: fail | step: utf8_decode | error: {e}")
            raise

        try:
            msg = json.loads(text)
        except json.JSONDecodeError as e:
            logging.error(f"action: receive_message | result: fail | step: json_decode | error: {e}")
            raise

        logging.info(f"action: receive_message | result: success | step: framed_read | length: {length}")
        return msg

    finally:
        # restaurar timeout original
        try:
            client_sock.settimeout(prev_timeout)
        except Exception:
            pass

def validate_bet(bet):
    if bet.agency == '':
        logging.error(f"action: validate_bet | result: fail | step: invalid_agency | agency: {bet.agency}")
        raise ValueError(f"invalid agency number: {bet.agency}")

    if bet.birthdate > datetime.date.today():
        logging.error(f"action: validate_bet | result: fail | step: invalid_birthdate | birthdate: {bet.birthdate}")
        raise ValueError(f"invalid birthdate: {bet.birthdate}")

    if bet.first_name == "" or bet.last_name == "":
        logging.error(f"action: validate_bet | result: fail | step: invalid_name | first_name: {bet.first_name} | last_name: {bet.last_name}")
        raise ValueError(f"invalid name: {bet.first_name} {bet.last_name}")

    logging.info(f"action: validate_bet | result: success | bet: {bet}")

    return True

def send_bet_confirmation(client_sock, bet):
    try:
        ack = {
            "v": 1,
            "type": "ack",
            "ok": True,
            "dni": bet.get("dni"),
            "numero": bet.get("numero"),
        }
        confirmation_msg = json.dumps(ack, ensure_ascii=False).encode("utf-8")
        confirmation_header = len(confirmation_msg).to_bytes(4, byteorder="big", signed=False)
        client_sock.sendall(confirmation_header + confirmation_msg)
        logging.info(f'action: send_ack | result: success | dni: {ack["dni"]} | numero: {ack["numero"]}')
    except Exception as e:
        logging.error(f'action: send_ack | result: fail | error: {e}')

def send_batch_ack_success(client_sock, count: int) -> None:
    """
    Envía ACK de batch exitoso:
      {"v":1,"type":"ack_batch","ok":true,"count":N}
    """
    ack = {"v": 1, "type": "ack_batch", "ok": True, "count": count}
    payload = json.dumps(ack, ensure_ascii=False).encode("utf-8")
    header = len(payload).to_bytes(4, "big", signed=False)
    client_sock.sendall(header + payload)
    logging.info(f'action: send_ack | result: success | type: ack_batch | count: {count}')

def send_batch_ack_fail(client_sock, count: int, code: str, reason: str) -> None:
    """
    Envía NACK de batch:
      {"v":1,"type":"ack_batch","ok":false,"count":N,"code":"...","reason":"..."}
    """
    ack = {
        "v": 1,
        "type": "ack_batch",
        "ok": False,
        "count": count,
        "code": code,
        "reason": reason,
    }
    payload = json.dumps(ack, ensure_ascii=False).encode("utf-8")
    header = len(payload).to_bytes(4, "big", signed=False)
    client_sock.sendall(header + payload)
    logging.info(f'action: send_ack | result: success | type: ack_batch | count: {count} | step: nack_sent')
