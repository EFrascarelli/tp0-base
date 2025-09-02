package common

import (
	"fmt"
	"net"
	"time"
	"encoding/json"
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

func (c *Client) sendBatch(ctx context.Context, items []Bet) error {

	// 1) serializar payload
	msg := batchMsg{V: 1, Type: "bets_batch", Items: items}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}


	
	// 2) escribir frame (4B big-endian + body)
	_ = c.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := writeFrame(c.conn, data); err != nil {
		return err
	}

	// 3) leer respuesta y VALIDAR ack de batch
	_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	body, err := readFrame(c.conn, 16*1024)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Errorf("action: receive_ack | result: fail | step: read_frame | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	var ack AckBatch
	if err := json.Unmarshal(body, &ack); err != nil {
		log.Errorf("action: receive_ack | result: fail | step: json_unmarshal | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	if ack.Type != "ack_batch" {
		err := fmt.Errorf("unexpected ack type: %s", ack.Type)
		log.Errorf("action: receive_ack | result: fail | step: bad_type | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	if !ack.OK {
		err := fmt.Errorf("%s: %s", ack.Code, ack.Reason)
		log.Errorf("action: receive_ack | result: fail | step: nack | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	// coherencia opcional de cantidad
	count := ack.Count
	if count == 0 {
		count = len(items)
	} else if count != len(items) {
		err := fmt.Errorf("ack count mismatch: got %d want %d", ack.Count, len(items))
		log.Errorf("action: receive_ack | result: fail | step: count_mismatch | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	log.Infof("action: receive_ack | result: success | type: ack_batch | client_id: %v | count: %d", c.config.ID, count)
	return nil

		// 4) listo (más adelante: validar ack_batch / ok:true / count, etc.)
		return nil
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

    batchMax := 50 // luego lo leeremos de config.yaml

    for i := 0; i < len(bets); i += batchMax {
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

        // Log de negocio para batch
        log.Infof("action: apuesta_enviada | result: success | cantidad: %d", len(chunk))
    }

    log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
}

