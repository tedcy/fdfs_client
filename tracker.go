package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"net"
	"fmt"
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
	if err := this.Header.RecvHeader(conn);err != nil {
		return fmt.Errorf("TrackerTask RecvHeader %v",err)
    }
	return nil
}

func (this *TrackerTask) RecvStorageInfo(conn net.Conn,pkgLen int) error {
	if pkgLen != 39 && pkgLen != 40 {
		return fmt.Errorf("RecvStorageInfo pkgLen %d invaild",pkgLen)
    }
	buf := make([]byte, pkgLen)
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
	if pkgLen == 40 {
		storePathIndex, err := buffer.ReadByte()
		if err != nil {
			return err
		}
		this.storePathIndex = int8(storePathIndex)
    }
	return nil
}
