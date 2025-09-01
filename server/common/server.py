import socket
import logging
import signal

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
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
        while self._is_running:
            client_sock = self.__accept_new_connection()
            if client_sock:
                self.__handle_client_connection(client_sock)
            else:
                logging.warning('action: wait_for_connection | result: timeout')

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            # TODO: Modify the receive to avoid short-reads
            msg = client_sock.recv(1024).rstrip().decode('utf-8')
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
            # TODO: Modify the send to avoid short-writes
            client_sock.send("{}\n".format(msg).encode('utf-8'))
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
        logging.info(f'action: shutdown | result: success')