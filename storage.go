package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"bufio"
)

type storageUploadTask struct {
	header
	//req
	fileInfo				*fileInfo
	storagePathIndex		int8
	//res
	fileId					string
}

func (this *storageUploadTask) SendReq(conn net.Conn) error {
	this.cmd = STORAGE_PROTO_CMD_UPLOAD_FILE
	this.pkgLen = this.fileInfo.fileSize + 15

	if err := this.SendHeader(conn);err != nil {
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

	var err error
	//send file
	if this.fileInfo.file != nil {
		_, err = conn.(pConn).Conn.(*net.TCPConn).ReadFrom(this.fileInfo.file)
    }else {
		_, err = conn.Write(this.fileInfo.buffer)
    }

	if err != nil {
		return err
	}
	return nil
}

func (this *storageUploadTask) RecvRes(conn net.Conn) error {
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
	groupName, err := readCStrFromByteBuffer(buffer, 16)
	if err != nil {
		return err
	}
	remoteFileName, err := readCStrFromByteBuffer(buffer, int(this.pkgLen)-16)
	if err != nil {
		return err
	}

	this.fileId = groupName + "/" + remoteFileName
	return nil
}

type storageDownloadTask struct {
	header
	//req
	groupName				string
	remoteFilename			string
	offset					int64
	downloadBytes			int64
	//res
	localFilename			string
    buffer					[]byte
}

func (this *storageDownloadTask) SendReq(conn net.Conn) error{
	this.cmd = STORAGE_PROTO_CMD_DOWNLOAD_FILE
	this.pkgLen = int64(len(this.remoteFilename) + 32)

	if err := this.SendHeader(conn);err != nil {
		return err
    }
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, this.offset); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, this.downloadBytes); err != nil {
		return err
	}
	byteGroupName := []byte(this.groupName)
	var bufferGroupName [16]byte
	for i := 0; i < len(byteGroupName);i++ {
		bufferGroupName[i] = byteGroupName[i]
	}
	buffer.Write(bufferGroupName[:])
	buffer.WriteString(this.remoteFilename)
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (this *storageDownloadTask) RecvRes(conn net.Conn) error{
	if err := this.RecvHeader(conn);err != nil {
		return fmt.Errorf("StorageDownloadTask RecvRes %v",err)
    }
	if this.localFilename != "" {
		if err := this.recvFile(conn);err != nil {
			return fmt.Errorf("StorageDownloadTask RecvRes %v",err)
        }
    }else {
		if err := this.recvBuffer(conn);err != nil {
			return fmt.Errorf("StorageDownloadTask RecvRes %v",err)
        }
    }
	return nil
}

func (this *storageDownloadTask) recvFile(conn net.Conn) error{
	file, err := os.Create(this.localFilename)
	defer file.Close()
	if err != nil {
		return err
    }

	writer := bufio.NewWriter(file)
	
	if err := writeFromConn(conn, writer, this.pkgLen);err != nil {
		return fmt.Errorf("StorageDownloadTask RecvFile %v",err)
    }
	if err := writer.Flush();err != nil {
		return fmt.Errorf("StorageDownloadTask RecvFile %v",err)
    }
	return nil
}

func (this *storageDownloadTask) recvBuffer(conn net.Conn) error{
	writer := new(bytes.Buffer)
	
	if err := writeFromConn(conn, writer, this.pkgLen);err != nil {
		return fmt.Errorf("StorageDownloadTask RecvBuffer %v",err)
    }
	this.buffer = writer.Bytes()
	return nil
}

type storageDeleteTask struct {
	header
	//req
	groupName				string
	remoteFilename			string
}

func (this *storageDeleteTask) SendReq(conn net.Conn) error{
	this.cmd = STORAGE_PROTO_CMD_DELETE_FILE
	this.pkgLen = int64(len(this.remoteFilename) + 16)

	if err := this.SendHeader(conn);err != nil {
		return err
    }
	buffer := new(bytes.Buffer)
	byteGroupName := []byte(this.groupName)
	var bufferGroupName [16]byte
	for i := 0; i < len(byteGroupName);i++ {
		bufferGroupName[i] = byteGroupName[i]
	}
	buffer.Write(bufferGroupName[:])
	buffer.WriteString(this.remoteFilename)
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (this *storageDeleteTask) RecvRes(conn net.Conn) error {
	return this.RecvHeader(conn)
}
