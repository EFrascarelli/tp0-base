import socket
import logging
import signal
from common.protocol import get_bet, send_bet_confirmation, send_batch_ack_fail, send_batch_ack_success
from common.utils import Bet, load_bets, store_bets, has_won

class Server:

    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._is_running = True
        self._connections = []
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def run(self):

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
        while self._is_running:
            client_sock = self.__accept_new_connection()
            if client_sock:
                self.__handle_client_connection(client_sock)
            else:
                logging.warning('action: wait_for_connection | step: timeout | result: in_progress')

    def __handle_client_connection(self, client_sock):
        try:
            msg_type, payload = get_bet(client_sock)
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg_type: {msg_type}')

            if msg_type == "batch":
                items = payload 
                n = len(items)

                try:
                    bets = [
                        Bet(
                            document=it.get("document"),
                            number=it.get("number"),
                            first_name=it.get("first_name"),
                            last_name=it.get("last_name"),
                            birthdate=it.get("birthdate"),
                            agency=it.get("agency"),
                        )
                        for it in items
                    ]           # mapear a atributos que espera store_bets
                    store_bets(bets)                               # persistir TODAS juntas (todo-o-nada)
                    logging.info(f'action: apuesta_recibida | result: success | cantidad: {n}')
                    send_batch_ack_success(client_sock, n)
                except Exception as e:
                    logging.error(f'action: apuesta_recibida | result: fail | cantidad: {n} | error: {e}')
                    send_batch_ack_fail(client_sock, n, code="STORE_FAILED", reason=str(e))
                return

            else:
                bet_dict = payload
                try:
                    bet = Bet(
                        bet_dict.get("agency"),
                        bet_dict.get("first_name"),
                        bet_dict.get("last_name"),
                        bet_dict.get("document"),
                        bet_dict.get("birthdate"),
                        bet_dict.get("number"),
                    )
                    store_bets([bet])
                    logging.info(f'action: apuesta_almacenada | result: success | dni: {bet_dict["document"]} | numero: {bet_dict["number"]}')
                except Exception as e:
                    logging.error(f'action: apuesta_almacenada | result: fail | dni: {bet_dict.get("document")} | numero: {bet_dict.get("number")} | error: {e}')
                    return

                # ACK individual (mantener lo que ya tenías)
                send_bet_confirmation(client_sock, bet_dict)
        except OSError as e:
            logging.error(f'action: receive_message | result: fail | error: {e}')
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        try:
            c, addr = self._server_socket.accept()
            self._connections.append(c)
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except Exception as e:
            logging.error(f'action: accept_connections | result: fail | error: {e}')
            return None

    def __signal_handler(self, signum, frame):
        logging.info(f'action: shutdown | result: in_progress')
        self._is_running = False
        for sock in self._connections:
            try:
                sock.close()
                logging.info("action: close_connection | result: success")
            except Exception as e:
                logging.error(f"action: close_connection | result: fail | error: {e}")
        self._server_socket.close()
        logging.info('action: exit | result: success')