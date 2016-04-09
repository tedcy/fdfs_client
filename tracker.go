package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"net"
)

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
	return this.Header.SendHeader(conn)
}

func (this *TrackerUploadTask) RecvHeader(conn net.Conn) error {
	return this.Header.RecvHeader(conn)
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
