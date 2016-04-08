package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"net"
)

type Header struct {
	pkgLen int64
	cmd    int8
	status int8
}

type TrackerUploadBody struct {
	groupName      string
	ipAddr         string
	port           int64
	storePathIndex int8
}

type TrackerUploadTask struct {
	Header
	TrackerUploadBody
}

func (this *TrackerUploadTask) SendHeader(conn net.Conn) error {
	this.cmd = 101

	buffer := new(bytes.Buffer)
	nouseBytes := make([]byte, 8)
	if _, err := buffer.Write(nouseBytes); err != nil {
		return err
	}
	if err := buffer.WriteByte(byte(this.cmd)); err != nil {
		return err
	}
	if err := buffer.WriteByte(byte(nouseBytes[0])); err != nil {
		return err
	}

	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (this *TrackerUploadTask) RecvHeader(conn net.Conn) error {
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

func (this *TrackerUploadTask) RecvBody(conn net.Conn) error {
	buf := make([]byte, 40)
	if _, err := conn.Read(buf); err != nil {
		return err
	}

	buffer := bytes.NewBuffer(buf)
	var err error
	this.groupName, err = ReadCStrFromByteBuffer(buffer, 16)
	if err != nil {
		return err
	}
	this.ipAddr, err = ReadCStrFromByteBuffer(buffer, 15)
	if err != nil {
		return err
	}
	if err := binary.Read(buffer, binary.BigEndian, &this.port); err != nil {
		return err
	}
	storePathIndex, err := buffer.ReadByte()
	if err != nil {
		return err
	}
	this.storePathIndex = int8(storePathIndex)
	return nil
}
