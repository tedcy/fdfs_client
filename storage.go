package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"bufio"
)

type StorageUploadTask struct {
	Header
	queryResult
	//req
	fileInfo *FileInfo
	storagePathIndex int8
	//res
	fileId *FileId
}

func (this *StorageUploadTask) sendReq(conn net.Conn) error {
	this.cmd = STORAGE_PROTO_CMD_UPLOAD_FILE
	this.pkgLen = this.fileInfo.fileSize + 15

	if err := this.Header.SendHeader(conn);err != nil {
		return err
    }
	buffer := new(bytes.Buffer)
	buffer.WriteByte(byte(this.storagePathIndex))
	if err := binary.Write(buffer, binary.BigEndian, this.fileInfo.fileSize); err != nil {
		return err
	}

	byteFileExtName := []byte(this.fileInfo.fileExtName)
	var bufferFileExtName [6]byte
	for i := 0; i < len(byteFileExtName); i++ {
		bufferFileExtName[i] = byteFileExtName[i]
	}
	buffer.Write(bufferFileExtName[:])

	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}

	//send file
	_, err := conn.(pConn).Conn.(*net.TCPConn).ReadFrom(this.fileInfo.file)

	if err != nil {
		return err
	}
	return nil
}

func (this *StorageUploadTask) recvRes(conn net.Conn) error {
	if err := this.RecvHeader(conn);err != nil {
		return err
    }

	if this.pkgLen <= 16 {
		return fmt.Errorf("recv file id pkgLen <= FDFS_GROUP_NAME_MAX_LEN")
	}
	if this.pkgLen > 100 {
		return fmt.Errorf("recv file id pkgLen > 100,can't be so long")
	}

	buf := make([]byte, this.pkgLen)
	if _,err := conn.Read(buf);err != nil {
		return err
    }

	buffer := bytes.NewBuffer(buf)
	groupName, err := ReadCStrFromByteBuffer(buffer, 16)
	if err != nil {
		return err
	}
	remoteFileName, err := ReadCStrFromByteBuffer(buffer, int(this.pkgLen)-16)
	if err != nil {
		return err
	}

	this.fileId = &FileId{
		GroupName:      groupName,
		RemoteFileName: remoteFileName,
	}
	return nil
}

type StorageDownloadTask struct {
	Header
}

func (this *StorageDownloadTask) SendHeader(conn net.Conn,groupName string,remoteFilename string,offset int64,downloadBytes int64) error{
	this.cmd = STORAGE_PROTO_CMD_DOWNLOAD_FILE
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

type StorageDeleteTask struct {
	Header
}

func (this *StorageDeleteTask) SendHeader(conn net.Conn,groupName string,remoteFilename string) error{
	this.cmd = STORAGE_PROTO_CMD_DELETE_FILE
	this.pkgLen = int64(len(remoteFilename) + 16)

	if err := this.Header.SendHeader(conn);err != nil {
		return err
    }
	buffer := new(bytes.Buffer)
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

func (this *StorageDeleteTask) RecvResult(conn net.Conn) error {
	return this.RecvHeader(conn)
}
