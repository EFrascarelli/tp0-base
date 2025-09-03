package common

import (
	"context"
	"encoding/binary"
	"bufio"
	"bytes"
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

type BatchMsg struct {
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

// --- helpers de protocolo textual (escape / unescape / writeLine / readLine) ---

func escape(s string) string {
    s = strings.ReplaceAll(s, `\`, `\\`)
    s = strings.ReplaceAll(s, `|`, `\|`)
    s = strings.ReplaceAll(s, "\n", `\n`)
    return s
}

func unescape(s string) string {
    var b strings.Builder
    for i := 0; i < len(s); {
        if s[i] == '\\' && i+1 < len(s) {
            switch s[i+1] {
            case '\\':
                b.WriteByte('\\'); i += 2; continue
            case '|':
                b.WriteByte('|'); i += 2; continue
            case 'n':
                b.WriteByte('\n'); i += 2; continue
            }
        }
        b.WriteByte(s[i])
        i++
    }
    return b.String()
}

// Escribe una línea asegurando short-write safe.
func writeLine(conn net.Conn, line string) error {
    if !strings.HasSuffix(line, "\n") {
        line += "\n"
    }
    buf := []byte(line)
    written := 0
    for written < len(buf) {
        n, err := conn.Write(buf[written:])
        if err != nil {
            return err
        }
        if n == 0 {
            return fmt.Errorf("short write: wrote 0 bytes")
        }
        written += n
    }
    return nil
}

// Lee una línea terminada en '\n' (maneja short-reads)
func readLine(conn net.Conn) (string, error) {
    r := bufio.NewReader(conn)
    line, err := r.ReadBytes('\n')
    if err != nil {
        return "", err
    }
    line = bytes.TrimSuffix(line, []byte{'\n'})
    return string(line), nil
}

func (c *Client) sendBatch(ctx context.Context, items []Bet) error {
    agID := 0
    if id, err := strconv.Atoi(c.config.ID); err == nil {
        agID = id
    }

    // 1) construir body textual: 
    //    primera línea:  BATCH|<count>\n
    //    luego N líneas: BET|... (una por apuesta) + '\n'
    var b strings.Builder
    b.WriteString(fmt.Sprintf("BATCH|%d\n", len(items)))
    for _, it := range items {
        b.WriteString(betLine(it, agID))
        b.WriteByte('\n')
    }
    body := []byte(b.String())

    // 2) enviar frame (4B + body)
    _ = c.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
    if err := writeFrame(c.conn, body); err != nil {
        if ctx.Err() != nil {
            return ctx.Err()
        }
        log.Errorf("action: send_batch | result: fail | step: write_frame | client_id: %v | error: %v", c.config.ID, err)
        return err
    }

    // 3) leer ACK del batch (formato textual con framing):
    //    éxito: ACKB|OK|<count>\n
    //    error:  ACKB|FAIL|<code>|<reason>\n
    _ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
    resp, err := readFrame(c.conn, 16*1024)
    if err != nil {
        if ctx.Err() != nil {
            return ctx.Err()
        }
        log.Errorf("action: receive_ack | result: fail | step: read_frame | client_id: %v | error: %v", c.config.ID, err)
        return err
    }

    line := strings.TrimSuffix(string(resp), "\n")
    parts := strings.Split(line, "|")
    if len(parts) < 2 || parts[0] != "ACKB" {
        e := fmt.Errorf("bad ack batch line: %q", line)
        log.Errorf("action: receive_ack | result: fail | step: bad_type | client_id: %v | error: %v", c.config.ID, e)
        return e
    }

    switch parts[1] {
    case "OK":
        if len(parts) != 3 {
            e := fmt.Errorf("bad ackb ok fields: %q", line)
            log.Errorf("action: receive_ack | result: fail | step: bad_fields | client_id: %v | error: %v", c.config.ID, e)
            return e
        }
        cnt, conv := strconv.Atoi(parts[2])
        if conv != nil {
            log.Errorf("action: receive_ack | result: fail | step: parse_count | client_id: %v | value: %q | error: %v", c.config.ID, parts[2], conv)
            return conv
        }
        // coherencia opcional
        if cnt != len(items) {
            e := fmt.Errorf("ack count mismatch: got %d want %d", cnt, len(items))
            log.Errorf("action: receive_ack | result: fail | step: count_mismatch | client_id: %v | error: %v", c.config.ID, e)
            return e
        }
        log.Infof("action: receive_ack | result: success | type: ack_batch | client_id: %v | count: %d", c.config.ID, cnt)
        return nil

    case "FAIL":
        if len(parts) != 4 {
            e := fmt.Errorf("bad ackb fail fields: %q", line)
            log.Errorf("action: receive_ack | result: fail | step: bad_fields | client_id: %v | error: %v", c.config.ID, e)
            return e
        }
        code := parts[2]
        reason := unescape(parts[3])
        e := fmt.Errorf("%s: %s", code, reason)
        log.Errorf("action: receive_ack | result: fail | step: nack | client_id: %v | error: %v", c.config.ID, e)
        return e

    default:
        e := fmt.Errorf("unexpected ackb status: %s", parts[1])
        log.Errorf("action: receive_ack | result: fail | step: bad_type | client_id: %v | error: %v", c.config.ID, e)
        return e
    }
}

func (c *Client) fitBatchBySize(items []Bet, maxBytes int) ([]Bet, int, error) {
    agID := 0
    if id, err := strconv.Atoi(c.config.ID); err == nil {
        agID = id
    }

    // probamos con n = len(items) hacia abajo hasta que el "body" textual entre en maxBytes
    for n := len(items); n > 0; n-- {
        // tamaño del header textual del batch: "BATCH|<n>\n"
        header := fmt.Sprintf("BATCH|%d\n", n)
        size := len(header)

        // sumar todas las líneas BET + '\n'
        fits := true
        for i := 0; i < n; i++ {
            line := betLine(items[i], agID)
            size += len(line) + 1 // + '\n'
            if size > maxBytes {
                fits = false
                break
            }
        }
        if fits {
            return items[:n], size, nil // 'size' = bytes del body textual (sin contar los 4B del frame)
        }
    }
    return nil, 0, fmt.Errorf("single_too_large")
}

func (c *Client) sendBet(ctx context.Context, nombre, apellido, dni, nacimiento string, numero int) error {
    // 1) armar línea textual: BET|dni|numero|nombre|apellido|nacimiento|agencia_id
    agID := 0
    if id, err := strconv.Atoi(c.config.ID); err == nil {
        agID = id
    }
    line := fmt.Sprintf("BET|%s|%d|%s|%s|%s|%d",
        escape(dni),
        numero,
        escape(nombre),
        escape(apellido),
        escape(nacimiento),
        agID,
    )

    // 2) enviar en un frame (4 bytes + payload textual con \n final)
    payload := []byte(line + "\n")
    _ = c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
    if err := writeFrame(c.conn, payload); err != nil {
        if ctx.Err() != nil {
            return ctx.Err()
        }
        log.Errorf("action: send_bet | result: fail | step: write_frame | client_id: %v | error: %v", c.config.ID, err)
        return err
    }
    log.Infof("action: send_bet | result: success | step: write_full | client_id: %v | bytes: %d", c.config.ID, len(payload))

    // 3) leer ACK en un frame y parsear textual:
    //    Formato éxito: ACK|OK|<dni>|<numero>
    //    Formato error: ACK|FAIL|<code>|<reason>
    _ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
    body, err := readFrame(c.conn, 16*1024)
    if err != nil {
        if ctx.Err() != nil {
            return ctx.Err()
        }
        log.Errorf("action: receive_ack | result: fail | step: read_frame | client_id: %v | error: %v", c.config.ID, err)
        return err
    }

    // convertir body a línea
    resp := strings.TrimSuffix(string(body), "\n")
    parts := strings.Split(resp, "|")
    if len(parts) < 2 || parts[0] != "ACK" {
        e := fmt.Errorf("bad ack line: %q", resp)
        log.Errorf("action: receive_ack | result: fail | step: bad_type | client_id: %v | error: %v", c.config.ID, e)
        return e
    }

    switch parts[1] {
    case "OK":
        if len(parts) != 4 {
            e := fmt.Errorf("bad ack ok fields: %q", resp)
            log.Errorf("action: receive_ack | result: fail | step: bad_fields | client_id: %v | error: %v", c.config.ID, e)
            return e
        }
        ackDNI := unescape(parts[2])
        ackNum, convErr := strconv.Atoi(parts[3])
        if convErr != nil {
            log.Errorf("action: receive_ack | result: fail | step: parse_num | client_id: %v | value: %q | error: %v", c.config.ID, parts[3], convErr)
            return convErr
        }
        log.Infof("action: receive_ack | result: success | client_id: %v | dni: %s | numero: %d", c.config.ID, ackDNI, ackNum)
        return nil

    case "FAIL":
        if len(parts) != 4 {
            e := fmt.Errorf("bad ack fail fields: %q", resp)
            log.Errorf("action: receive_ack | result: fail | step: bad_fields | client_id: %v | error: %v", c.config.ID, e)
            return e
        }
        code := parts[2]
        reason := unescape(parts[3])
        e := fmt.Errorf("%s: %s", code, reason)
        log.Errorf("action: receive_ack | result: fail | step: nack | client_id: %v | error: %v", c.config.ID, e)
        return e

    default:
        e := fmt.Errorf("unexpected ack status: %s", parts[1])
        log.Errorf("action: receive_ack | result: fail | step: bad_type | client_id: %v | error: %v", c.config.ID, e)
        return e
    }
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

func betLine(b Bet, agID int) string {
    // BET|dni|numero|nombre|apellido|nacimiento|agencia_id
    return fmt.Sprintf("BET|%s|%d|%s|%s|%s|%d",
        escape(b.DNI),
        b.Numero,
        escape(b.Nombre),
        escape(b.Apellido),
        escape(b.Nacimiento),
        agID,
    )
}