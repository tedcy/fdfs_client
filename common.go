package fdfs_client

import (
	"os"
	"net"
	"bytes"
	"encoding/binary"
)

const (
	FDFS_PROTO_CMD_ACTIVE_TEST			= 111
	TRACKER_PROTO_CMD_RESP				= 100
)

type FileId struct {
	GroupName		string
	RemoteFileName  string
}

type StorageInfo struct {
	addr             string
	storagePathIndex int8
}

type FileInfo struct {
	fileSize    int64
	file        *os.File
	fileExtName string
}

type Header struct {
	pkgLen int64
	cmd    int8
	status int8
}

func (this *Header) SendHeader(conn net.Conn) error {
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, this.pkgLen); err != nil {
		return err
	}
	if err := buffer.WriteByte(byte(this.cmd)); err != nil {
		return err
	}
	if err := buffer.WriteByte(byte(this.status)); err != nil {
		return err
	}

	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (this *Header) RecvHeader(conn net.Conn) error {
	buf := make([]byte, 10)
	if _, err := conn.Read(buf); err != nil {
		return err
	}

	buffer := bytes.NewBuffer(buf)

	if err := binary.Read(buffer, binary.BigEndian, &this.pkgLen); err != nil {
		return err
	}
	cmd, err := buffer.ReadByte()
	if err != nil {
		return err
	}
	status, err := buffer.ReadByte()
	if err != nil {
		return err
	}
	this.cmd = int8(cmd)
	this.status = int8(status)
	return nil
}
