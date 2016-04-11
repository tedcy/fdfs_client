package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"bufio"
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

	if err := this.Header.SendHeader(conn);err != nil {
		return err
    }
	buffer := new(bytes.Buffer)
	buffer.WriteByte(byte(this.storagePathIndex))
	if err := binary.Write(buffer, binary.BigEndian, this.fileSize); err != nil {
		return err
	}

	byteFileExtName := []byte(fileInfo.fileExtName)
	for i := 0; i < len(byteFileExtName); i++ {
		this.fileExtName[i] = byteFileExtName[i]
	}
	buffer.Write(this.fileExtName[:])

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
	if err := this.RecvHeader(conn);err != nil {
		return nil,err
    }

	if this.pkgLen <= 16 {
		return nil, fmt.Errorf("recv file id pkgLen <= FDFS_GROUP_NAME_MAX_LEN")
	}
	if this.pkgLen > 100 {
		return nil, fmt.Errorf("recv file id pkgLen > 100,can't be so long")
	}

	buf := make([]byte, this.pkgLen)
	if _,err := conn.Read(buf);err != nil {
		return nil,err
    }

	buffer := bytes.NewBuffer(buf)
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

type StorageDownloadTask struct {
	Header
}

func (this *StorageDownloadTask) SendHeader(conn net.Conn,groupName string,remoteFilename string,offset int64,downloadBytes int64) error{
	this.cmd = 14
	this.pkgLen = int64(len(remoteFilename) + 32)

	if err := this.Header.SendHeader(conn);err != nil {
		return err
    }
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, offset); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, downloadBytes); err != nil {
		return err
	}
	byteGroupName := []byte(groupName)
	var bufferGroupName [16]byte
	for i := 0; i < len(byteGroupName);i++ {
		bufferGroupName[i] = byteGroupName[i]
	}
	buffer.Write(bufferGroupName[:])
	buffer.WriteString(remoteFilename)
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (this *StorageDownloadTask) RecvFile(conn net.Conn,localFilename string) error{
	if err := this.RecvHeader(conn);err != nil {
		return fmt.Errorf("StorageDownloadTask RecvFile %v",err)
    }
	file, err := os.Create(localFilename)
	defer file.Close()
	if err != nil {
		return err
    }

	writer := bufio.NewWriter(file)
	
	if err := WriteFromConn(conn, writer, this.pkgLen);err != nil {
		return fmt.Errorf("StorageDownloadTask RecvFile %v",err)
    }
	if err := writer.Flush();err != nil {
		return fmt.Errorf("StorageDownloadTask RecvFile %v",err)
    }
	return nil
}

func (this *StorageDownloadTask) RecvBuffer(conn net.Conn) ([]byte,error){
	if err := this.RecvHeader(conn);err != nil {
		return nil,fmt.Errorf("StorageDownloadTask RecvBuffer %v",err)
    }

	writer := new(bytes.Buffer)
	
	if err := WriteFromConn(conn, writer, this.pkgLen);err != nil {
		return nil,fmt.Errorf("StorageDownloadTask RecvBuffer %v",err)
    }
	return writer.Bytes(),nil
}
