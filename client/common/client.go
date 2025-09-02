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

        if err := c.createClientSocket(); err != nil {
            return
        }
        if err := c.sendBatch(ctx, chunk); err != nil {
            if ctx.Err() != nil {
                log.Infof("action: send_batch | result: success | step: cancelled | client_id: %v", c.config.ID)
            } else {
                log.Errorf("action: send_batch | result: fail | step: io | client_id: %v | error: %v", c.config.ID, err)
            }
            _ = c.conn.Close()
            c.conn = nil
            return
        }
        _ = c.conn.Close()
        c.conn = nil

		i+=len(chunk)

        // Log de negocio para batch
        log.Infof("action: apuesta_enviada | result: success | cantidad: %d", len(chunk))
    }

    log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
}

