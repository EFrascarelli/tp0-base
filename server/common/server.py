import socket
import logging
import signal
from server.common.protocol import get_bet, validate_bet, send_bet_confirmation, BetObj
from common.utils import load_bets, store_bets, has_won

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
            # TODO: Modify the receive to avoid short-reads


            bet = get_bet(client_sock)
            addr = client_sock.getpeername()
            
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | bet: {bet}')

            try:
                store_bets([BetObj(bet)])
                logging.info(f'action: apuesta_almacenada | result: success | dni: {bet["dni"]} | numero: {bet["numero"]}')
            except Exception as e:
                logging.error(f'action: apuesta_almacenada | result: fail | dni: {bet.get("dni")} | numero: {bet.get("numero")} | error: {e}')
                return
            
            send_bet_confirmation(client_sock, bet)
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