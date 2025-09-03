package common

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"time"
	"bufio"
	"bytes"
	"strings"
	"unicode/utf8"
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
	// Armar línea textual
	agID := 0
	if id, err := strconv.Atoi(c.config.ID); err == nil {
		agID = id
	}
	line := encodeBetLine(nombre, apellido, dni, nacimiento, numero, agID)
	data := []byte(line) // seguimos usando framing: mandamos la línea como payload

	// Escritura con deadlines cortos
	_ = c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := writeFrame(c.conn, data); err != nil {
		if ctx.Err() != nil {
			// cancelación por señal
			return ctx.Err()
		}
		log.Infof("action: send_bet | result: success | step: write_full | client_id: %v | bytes: %d", c.config.ID, len(data))
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

	ok, adni, anum, code, reason, perr := parseAckLine(string(body))
	if perr != nil {
		log.Errorf("action: receive_ack | result: fail | step: parse_ack | client_id: %v | error: %v", c.config.ID, perr)
		return perr
	}
	if !ok {
		err := fmt.Errorf("%s: %s", code, reason)
		log.Errorf("action: receive_ack | result: fail | step: nack | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	log.Infof("action: receive_ack | result: success | client_id: %v | dni: %s | numero: %d", c.config.ID, adni, anum)
	return nil
}

// escape: convierte |, \n y \ en secuencias escapadas para el protocolo textual
func escape(s string) string {
    s = strings.ReplaceAll(s, `\`, `\\`)
    s = strings.ReplaceAll(s, `|`, `\|`)
    s = strings.ReplaceAll(s, "\n", `\n`)
    return s
}

// writeLine: escribe una línea (con \n final) asegurando short-write safe
func writeLine(conn net.Conn, line string) error {
    if !strings.HasSuffix(line, "\n") {
        line += "\n"
    }
    b := []byte(line)
    written := 0
    for written < len(b) {
        n, err := conn.Write(b[written:])
        if err != nil {
            return err
        }
        written += n
    }
    return nil
}

// readLine: lee hasta '\n' (maneja short-reads)
func readLine(conn net.Conn) (string, error) {
    r := bufio.NewReader(conn)
    line, err := r.ReadBytes('\n')
    if err != nil {
        return "", err
    }
    // quitamos el '\n' final si está
    line = bytes.TrimSuffix(line, []byte{'\n'})
    return string(line), nil
}

// unescape: revierte \|, \n y \\ a sus valores reales
func unescape(s string) string {
	// Recorremos rune por rune para soportar UTF-8 (nombres con acentos).
	out := make([]rune, 0, len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == '\\' {
			// Mirar el siguiente
			if i+size < len(s) {
				r2, size2 := utf8.DecodeRuneInString(s[i+size:])
				switch r2 {
				case '\\':
					out = append(out, '\\')
					i += size + size2
					continue
				case '|':
					out = append(out, '|')
					i += size + size2
					continue
				case 'n':
					out = append(out, '\n')
					i += size + size2
					continue
				}
			}
			// Backslash suelto: lo conservamos
			out = append(out, r)
			i += size
			continue
		}
		out = append(out, r)
		i += size
	}
	return string(out)
}

// encodeBetLine: serializa una apuesta en línea de texto sin JSON.
// Formato: BET|dni|numero|nombre|apellido|nacimiento|agencia_id
func encodeBetLine(nombre, apellido, dni, nacimiento string, numero, agenciaID int) string {
	fields := []string{
		"BET",
		escape(dni),
		strconv.Itoa(numero),
		escape(nombre),
		escape(apellido),
		escape(nacimiento),
		strconv.Itoa(agenciaID),
	}
	return strings.Join(fields, "|")
}

// parseAckLine: parsea una respuesta de ACK textual.
// OK:    "ACK|OK|<dni>|<numero>"
// ERROR: "ACK|FAIL|<code>|<reason>"
func parseAckLine(line string) (ok bool, dni string, numero int, code, reason string, err error) {
	parts := strings.Split(line, "|")
	if len(parts) < 2 || parts[0] != "ACK" {
		return false, "", 0, "", "", fmt.Errorf("bad ack: %q", line)
	}
	switch parts[1] {
	case "OK":
		if len(parts) != 4 {
			return false, "", 0, "", "", fmt.Errorf("bad ack OK shape: %q", line)
		}
		dni = unescape(parts[2])
		n, convErr := strconv.Atoi(parts[3])
		if convErr != nil {
			return false, "", 0, "", "", convErr
		}
		return true, dni, n, "", "", nil
	case "FAIL":
		if len(parts) < 4 {
			return false, "", 0, "", "", fmt.Errorf("bad ack FAIL shape: %q", line)
		}
		code = parts[2]
		reason = unescape(parts[3])
		return false, "", 0, code, reason, nil
	default:
		return false, "", 0, "", "", fmt.Errorf("bad ack type: %q", parts[1])
	}
}