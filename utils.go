package fdfs_client

import (
	"bytes"
	"fmt"
	"net"
)

func readCStrFromByteBuffer(buffer *bytes.Buffer, size int) (string, error) {
	buf := make([]byte, size)
	if _, err := buffer.Read(buf); err != nil {
		return "", err
	}

	index := bytes.IndexByte(buf, 0x00)

	if index == -1 {
		return string(buf), nil
	}

	return string(buf[0:index]), nil
}

type writer interface {
	Write(p []byte) (int, error)
}

func writeFromConn(conn net.Conn, writer writer, size int64) error {
	sizeRecv, sizeAll := int64(0), size
	buf := make([]byte, 4096)
	for sizeRecv+4096 <= sizeAll {
		recv, err := conn.Read(buf)
		if err != nil {
			return err
		}
		if _, err := writer.Write(buf); err != nil {
			return err
		}
		sizeRecv += int64(recv)
	}
	buf = make([]byte, sizeAll-sizeRecv)
	recv, err := conn.Read(buf)
	if err != nil {
		return err
	}
	if int64(recv) < sizeAll-sizeRecv {
		return fmt.Errorf("recv %d expect %d", recv, sizeAll-sizeRecv)
	}
	if _, err := writer.Write(buf); err != nil {
		return err
	}

	return nil
}
