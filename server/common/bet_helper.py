from datetime import datetime
import json
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
    """
    Send a confirmation message back to the client after a successful bet placement.
    """
    confirmation_msg = json.dumps({"status": "success", "bet": bet})
    confirmation_msg = confirmation_msg.encode("utf-8")
    confirmation_header = len(confirmation_msg).to_bytes(4, byteorder="big")
    client_sock.sendall(confirmation_header + confirmation_msg)