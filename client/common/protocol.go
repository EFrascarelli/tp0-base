package common

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"os"
	"encoding/csv"
	"io"
)

type Bet struct {
	V          int    `json:"v"`
	Type       string `json:"type"`
	DNI        string `json:"dni"`
	Numero     int    `json:"numero"`
	Nombre     string `json:"nombre"`
	Apellido   string `json:"apellido"`
	Nacimiento string `json:"nacimiento"`
	AgenciaID  int    `json:"agencia_id,omitempty"`
}

type Ack struct {
	V      int    `json:"v"`
	Type   string `json:"type"`
	OK     bool   `json:"ok"`
	DNI    string `json:"dni,omitempty"`
	Numero int    `json:"numero,omitempty"`
	Code   string `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type batchMsg struct {
	V     int    `json:"v"`
	Type  string `json:"type"`
	Items []Bet  `json:"items"`
}

type AckBatch struct {
    V      int    `json:"v"`
    Type   string `json:"type"` // "ack_batch"
    OK     bool   `json:"ok"`
    Count  int    `json:"count,omitempty"`
    Code   string `json:"code,omitempty"`
    Reason string `json:"reason,omitempty"`
}

func writeFull(conn net.Conn, buf []byte) error {
	total := 0
	for total < len(buf) {
		n, err := conn.Write(buf[total:])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("short write: wrote 0 bytes")
		}
		total += n
	}
	return nil
}

func readExact(conn net.Conn, n int) ([]byte, error) {
	out := make([]byte, n)
	read := 0
	for read < n {
		m, err := conn.Read(out[read:])
		if err != nil {
			return nil, err
		}
		if m == 0 {
			return nil, fmt.Errorf("short read: peer closed")
		}
		read += m
	}
	return out, nil
}

func writeFrame(conn net.Conn, payload []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if err := writeFull(conn, header); err != nil {
		return err
	}
	return writeFull(conn, payload)
}

func readFrame(conn net.Conn, maxLen int) ([]byte, error) {
	hdr, err := readExact(conn, 4)
	if err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr))
	if n < 0 || n > maxLen {
		return nil, fmt.Errorf("invalid frame length: %d", n)
	}
	return readExact(conn, n)
}


func (c *Client) sendBet(ctx context.Context, nombre, apellido, dni, nacimiento string, numero int) error {
	// Armar payload
	b := Bet{
		V:          1,
		Type:       "bet",
		DNI:        dni,
		Numero:     numero,
		Nombre:     nombre,
		Apellido:   apellido,
		Nacimiento: nacimiento,
	}
	if id, err := strconv.Atoi(c.config.ID); err == nil {
		b.AgenciaID = id
	}

	data, err := json.Marshal(b)
	if err != nil {
		log.Errorf("action: send_bet | result: fail | step: json_marshal | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	// Escritura con deadlines cortos
	_ = c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := writeFrame(c.conn, data); err != nil {
		if ctx.Err() != nil {
			// cancelación por señal
			return ctx.Err()
		}
		log.Errorf("action: send_bet | result: fail | step: write_frame | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	log.Infof("action: send_bet | result: success | step: write_full | client_id: %v | bytes: %d", c.config.ID, len(data))

	// Lectura del ACK con deadline
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	body, err := readFrame(c.conn, 16*1024)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Errorf("action: receive_ack | result: fail | step: read_frame | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	var ack Ack
	if err := json.Unmarshal(body, &ack); err != nil {
		log.Errorf("action: receive_ack | result: fail | step: json_unmarshal | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	if ack.Type != "ack" {
		err := fmt.Errorf("unexpected ack type: %s", ack.Type)
		log.Errorf("action: receive_ack | result: fail | step: bad_type | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	if !ack.OK {
		err := fmt.Errorf("%s: %s", ack.Code, ack.Reason)
		log.Errorf("action: receive_ack | result: fail | step: nack | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	log.Infof("action: receive_ack | result: success | client_id: %v | dni: %s | numero: %d", c.config.ID, ack.DNI, ack.Numero)
	return nil
}

func (c *Client) getBetsFromCSV(path string) ([]Bet, error) {
    f, err := os.Open(path)
    if err != nil {
        log.Errorf("action: load_dataset | result: fail | step: open_file | path: %s | error: %v", path, err)
        return nil, err
    }
    defer f.Close()

    r := csv.NewReader(f)

    var out []Bet
    for {
        rec, err := r.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Errorf("action: load_dataset | result: fail | step: read_record | path: %s | error: %v", path, err)
            return nil, err
        }

        if len(rec) < 5 {
            log.Errorf("action: load_dataset | result: fail | step: missing_columns | record: %v", rec)
            return nil, fmt.Errorf("invalid record: not enough fields")
        }

        numero, err := strconv.Atoi(strings.TrimSpace(rec[4]))
        if err != nil {
            log.Errorf("action: load_dataset | result: fail | step: parse_number | value: %q | error: %v", rec[4], err)
            return nil, err
        }

        b := Bet{
            V:          1,
            Type:       "bet",
            Nombre:     strings.TrimSpace(rec[0]),
            Apellido:   strings.TrimSpace(rec[1]),
            DNI:        strings.TrimSpace(rec[2]),
            Nacimiento: strings.TrimSpace(rec[3]),
            Numero:     numero,
        }

        // Agencia: por convención, usar el ID del cliente
        if id, err := strconv.Atoi(c.config.ID); err == nil {
            b.AgenciaID = id
        }

        out = append(out, b)
    }

    log.Infof("action: load_dataset | result: success | path: %s | count: %d", path, len(out))
    return out, nil
}
