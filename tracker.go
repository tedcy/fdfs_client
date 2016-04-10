package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"net"
)

type TrackerStorageInfo struct {
	groupName      string
	ipAddr         string
	port           int64
	storePathIndex int8
}

type TrackerTask struct {
	Header
	TrackerStorageInfo
}

func (this *TrackerTask) SendHeader(conn net.Conn) error {
	return this.Header.SendHeader(conn)
}

func (this *TrackerTask) RecvHeader(conn net.Conn) error {
	return this.Header.RecvHeader(conn)
}

func (this *TrackerTask) RecvStorageInfo(conn net.Conn) error {
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
