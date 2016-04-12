package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type trackerStorageInfo struct {
	groupName      string
	ipAddr         string
	port           int64
	storePathIndex int8
}

type trackerTask struct {
	header
	//req
	groupName      string
	remoteFilename string
	//res
	trackerStorageInfo
}

func (this *trackerTask) SendReq(conn net.Conn) error {
	if this.groupName != "" {
		this.pkgLen = int64(FDFS_GROUP_NAME_MAX_LEN + len(this.remoteFilename))
	}
	if err := this.SendHeader(conn); err != nil {
		return err
	}
	if this.groupName != "" {
		buffer := new(bytes.Buffer)
		byteGroupName := []byte(this.groupName)
		var bufferGroupName [16]byte
		for i := 0; i < len(byteGroupName); i++ {
			bufferGroupName[i] = byteGroupName[i]
		}
		buffer.Write(bufferGroupName[:])
		buffer.WriteString(this.remoteFilename)
		if _, err := conn.Write(buffer.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (this *trackerTask) RecvRes(conn net.Conn) error {
	if err := this.RecvHeader(conn); err != nil {
		return fmt.Errorf("TrackerTask RecvHeader %v", err)
	}
	if this.pkgLen != 39 && this.pkgLen != 40 {
		return fmt.Errorf("recvStorageInfo pkgLen %d invaild", this.pkgLen)
	}
	buf := make([]byte, this.pkgLen)
	if _, err := conn.Read(buf); err != nil {
		return err
	}

	buffer := bytes.NewBuffer(buf)
	var err error
	this.groupName, err = readCStrFromByteBuffer(buffer, 16)
	if err != nil {
		return err
	}
	this.ipAddr, err = readCStrFromByteBuffer(buffer, 15)
	if err != nil {
		return err
	}
	if err := binary.Read(buffer, binary.BigEndian, &this.port); err != nil {
		return err
	}
	if this.pkgLen == 40 {
		storePathIndex, err := buffer.ReadByte()
		if err != nil {
			return err
		}
		this.storePathIndex = int8(storePathIndex)
	}
	return nil
}
