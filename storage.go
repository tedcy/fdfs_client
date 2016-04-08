package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type StorageUploadHeader struct {
	Header
	storagePathIndex int8
	fileSize         int64
	fileExtName      [6]byte
}

type StorageUploadTask struct {
	StorageUploadHeader
}

func (this *StorageUploadTask) SendHeader(conn net.Conn, fileInfo *FileInfo, storagePathIndex int8) error {
	this.cmd = 11
	this.pkgLen = fileInfo.fileSize + 15
	this.fileSize = fileInfo.fileSize
	this.storagePathIndex = storagePathIndex

	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, this.pkgLen); err != nil {
		return err
	}
	if err := buffer.WriteByte(byte(this.cmd)); err != nil {
		return err
	}
	if err := buffer.WriteByte(byte(0x00)); err != nil {
		return err
	}
	if err := buffer.WriteByte(byte(this.storagePathIndex)); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, this.fileSize); err != nil {
		return err
	}

	byteFileExtName := []byte(fileInfo.fileExtName)
	for i := 0; i < len(byteFileExtName); i++ {
		this.fileExtName[i] = byteFileExtName[i]
	}
	if _, err := buffer.Write(this.fileExtName[:]); err != nil {
		return err
	}

	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (this *StorageUploadTask) SendFile(conn net.Conn, fileInfo *FileInfo) error {
	_, err := conn.(pConn).Conn.(*net.TCPConn).ReadFrom(fileInfo.file)

	if err != nil {
		return err
	}
	return nil
}

func (this *StorageUploadTask) RecvFileId(conn net.Conn) (*FileId, error) {
	buf := make([]byte, 10)
	if _, err := conn.Read(buf); err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(buf)
	if err := binary.Read(buffer, binary.BigEndian, &this.pkgLen); err != nil {
		return nil, err
	}
	if _, err := buffer.ReadByte(); err != nil {
		return nil, err
	}
	status, err := buffer.ReadByte()
	if err != nil {
		return nil, err
	}
	this.status = int8(status)

	if this.status != 0 {
		return nil, fmt.Errorf("recv file id status != 0")
	}
	if this.pkgLen <= 16 {
		return nil, fmt.Errorf("recv file id pkgLen <= FDFS_GROUP_NAME_MAX_LEN")
	}
	if this.pkgLen > 100 {
		return nil, fmt.Errorf("recv file id pkgLen > 100,can't be so long")
	}

	buf = make([]byte, this.pkgLen)
	if _,err := conn.Read(buf);err != nil {
		return nil,err
    }

	buffer = bytes.NewBuffer(buf)
	groupName, err := ReadCStrFromByteBuffer(buffer, 16)
	if err != nil {
		return nil, err
	}
	remoteFileName, err := ReadCStrFromByteBuffer(buffer, int(this.pkgLen)-16)
	if err != nil {
		return nil, err
	}

	return &FileId{
		GroupName:      groupName,
		RemoteFileName: remoteFileName,
	}, nil
}
