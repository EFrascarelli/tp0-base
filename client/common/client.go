package common

import (
	"fmt"
	"net"
	"time"
    "context"
    "os/signal"
    "syscall"
	"github.com/op/go-logging"
	"os"
	"strconv"
)

var log = logging.MustGetLogger("log")


type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

type Client struct {
	config ClientConfig
	conn   net.Conn
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

func (c *Client) getEnvs() (nombre, apellido, dni, nacimiento string, numero int, err error) {
	// leer bet desde ENV
	nombre = os.Getenv("NOMBRE")
	apellido = os.Getenv("APELLIDO")
	dni = os.Getenv("DOCUMENTO")
	nacimiento = os.Getenv("NACIMIENTO")
	numStr := os.Getenv("NUMERO")

	// convertir NUMERO a int
	numero, convErr := strconv.Atoi(numStr)
	if convErr != nil {
		log.Errorf("action: config_apuesta | result: fail | step: parse_num | client_id: %v | value: %v | error: %v",
			c.config.ID, numStr, convErr)
		return "", "", "", "", 0, convErr
	}

	// validaciones mínimas
	if dni == "" || nombre == "" || apellido == "" || nacimiento == "" {
		log.Errorf("action: config_apuesta | result: fail | step: missing_field | client_id: %v | dni:%q nombre:%q apellido:%q nacimiento:%q",
			c.config.ID, dni, nombre, apellido, nacimiento)
		return "", "", "", "", 0, fmt.Errorf("missing required field")
	}

	// log de configuración exitosa
	log.Infof("action: config_apuesta | result: success | client_id: %v | dni: %s | numero: %d | nombre: %s | apellido: %s | nacimiento: %s",
		c.config.ID, dni, numero, nombre, apellido, nacimiento)

	return nombre, apellido, dni, nacimiento, numero, nil
}


func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) StartClientLoop() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	nombre, apellido, dni, nacimiento, numero, err := c.getEnvs()
	if err != nil {
		return
	}
	log.Infof("action: client_config | result: success | client_id: %v | dni: %s | numero: %d | nombre: %s | apellido: %s | nacimiento: %s",
		c.config.ID, dni, numero, nombre, apellido, nacimiento)

	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		// Permitir cancelación por señal antes de iniciar trabajo
		select {
		case <-ctx.Done():
			log.Infof("action: shutdown | result: success | step: stop_loop | client_id: %v", c.config.ID)
			return
		default:
		}

		if err := c.createClientSocket(); err != nil {
			return
		}

		if err := c.sendBet(ctx, nombre, apellido, dni, nacimiento, numero); err != nil {
			if ctx.Err() != nil {
				log.Infof("action: receive_ack | result: success | step: cancelled | client_id: %v", c.config.ID)
			} else {
				log.Errorf("action: send_bet | result: fail | step: io | client_id: %v | error: %v", c.config.ID, err)
			}
			_ = c.conn.Close()
			c.conn = nil
			return
		}

		log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %d", dni, numero)
		_ = c.conn.Close()
		c.conn = nil

		// Si hay más de una iteración, esperar entre envíos
		if c.config.LoopAmount > 1 {
			select {
			case <-ctx.Done():
				log.Infof("action: shutdown | result: success | step: break_sleep | client_id: %v", c.config.ID)
				return
			case <-time.After(c.config.LoopPeriod):
			}
		}
	}

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
	log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
}
