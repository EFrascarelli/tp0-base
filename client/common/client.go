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
	BatchMaxAmount int 
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


// No se borra esta funcion por si vuelve a utilizarse. Mientras tanto queda sin uso
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

    agencyPath := fmt.Sprintf("/data/agency-%s.csv", c.config.ID)
    bets, err := c.getBetsFromCSV(agencyPath)
    if err != nil {
        return
    }
    log.Infof("action: dataset_loaded | result: success | client_id: %v | path: %s | count: %d",
        c.config.ID, agencyPath, len(bets))

	batchMax := c.config.BatchMaxAmount
	if batchMax <= 0 {
		batchMax = 50 // default seguro
		log.Infof("action: config | result: success | step: batch_default | client_id: %v | batch_max: %d", c.config.ID, batchMax)
	}
	
    for i := 0; i < len(bets); {
        select {
        case <-ctx.Done():
            log.Infof("action: shutdown | result: success | step: stop_loop | client_id: %v", c.config.ID)
            return
        default:
        }

        end := i + batchMax
        if end > len(bets) {
            end = len(bets)
        }
        chunk := bets[i:end]


		const maxBodyBytes = 8000
		chunk, size, err := c.fitBatchBySize(chunk, maxBodyBytes)
		if err != nil {
			log.Errorf("action: send_batch | result: fail | step: single_too_large | client_id: %v", c.config.ID)
			return
		}
		if size > 0 && len(chunk) < (end-i) {
			log.Infof("action: send_batch | result: in_progress | step: shrink_by_bytes | bytes: %d | count: %d", size, len(chunk))
		}

		// abrir conexión por batch
		if err := c.createClientSocket(); err != nil {
			log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		// enviar batch
		if err := c.sendBatch(ctx, chunk); err != nil {
			if ctx.Err() != nil {
				// cancelado por señal → salir graceful
				log.Infof("action: shutdown | result: success | step: send_batch_cancelled | client_id: %v", c.config.ID)
			} else {
				log.Errorf("action: send_batch | result: fail | step: io | client_id: %v | error: %v", c.config.ID, err)
			}
			_ = c.conn.Close()
			c.conn = nil
			return // abortamos para no entrar en reintentos infinitos
		}



		// éxito → cerramos, avanzamos índice y registramos negocio
		log.Infof("action: apuesta_enviada | result: success | cantidad: %d", len(chunk))
		_ = c.conn.Close()
		c.conn = nil

		i += len(chunk) // ⚠️ avanzar al próximo segmento
    }

	if err := c.createClientSocket(); err != nil {
		log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if err := c.sendNotifyDone(ctx); err != nil {
		log.Errorf("action: finish | result: fail | client_id: %v | error: %v", c.config.ID, err)
		_ = c.conn.Close()
		c.conn = nil
		return
	}
	_ = c.conn.Close()
	c.conn = nil

	// consultar ganadores
	if err := c.createClientSocket(); err != nil {
		log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	agID, _ := strconv.Atoi(c.config.ID)
	count, _, err := c.sendWinnersQuery(ctx, agID)
	_ = c.conn.Close()
	c.conn = nil
	if err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", count)

    log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
}

