package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	TRACKER_PROTO_CMD_RESP                                  = 100
	TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101
	TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE               = 102

	STORAGE_PROTO_CMD_UPLOAD_FILE   = 11
	STORAGE_PROTO_CMD_DELETE_FILE   = 12
	STORAGE_PROTO_CMD_DOWNLOAD_FILE = 14
	FDFS_PROTO_CMD_ACTIVE_TEST      = 111
)

const (
	FDFS_GROUP_NAME_MAX_LEN = 16
)

type storageInfo struct {
	addr             string
	storagePathIndex int8
}

type fileInfo struct {
	fileSize    int64
	buffer      []byte
	file        *os.File
	fileExtName string
}

func newFileInfo(fileName string, buffer []byte, fileExtName string) (*fileInfo, error) {
	if fileName != "" {
		file, err := os.Open(fileName)
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		if int(stat.Size()) == 0 {
			return nil, fmt.Errorf("file %q size is zero", fileName)
		}
		var fileExtName string
		index := strings.LastIndexByte(fileName, '.')
		if index != -1 {
			fileExtName = fileName[index+1:]
			if len(fileExtName) > 6 {
				fileExtName = fileExtName[:6]
			}
		}
		return &fileInfo{
			fileSize:    stat.Size(),
			file:        file,
			fileExtName: fileExtName,
		}, nil
	}
	if len(fileExtName) > 6 {
		fileExtName = fileExtName[:6]
	}
	return &fileInfo{
		fileSize:    int64(len(buffer)),
		buffer:      buffer,
		fileExtName: fileExtName,
	}, nil
}

func (this *fileInfo) Close() {
	if this == nil {
		return
	}
	if this.file != nil {
		this.file.Close()
	}
	return
}

type task interface {
	SendReq(net.Conn) error
	RecvRes(net.Conn) error
}

type header struct {
	pkgLen int64
	cmd    int8
	status int8
}

func (this *header) SendHeader(conn net.Conn) error {
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, this.pkgLen); err != nil {
		return err
	}
	buffer.WriteByte(byte(this.cmd))
	buffer.WriteByte(byte(this.status))

	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (this *header) RecvHeader(conn net.Conn) error {
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
	if status != 0 {
		return fmt.Errorf("recv resp status %d != 0", status)
	}
	this.cmd = int8(cmd)
	this.status = int8(status)
	return nil
}

func splitFileId(fileId string) (string, string, error) {
	str := strings.SplitN(fileId, "/", 2)
	if len(str) < 2 {
		return "", "", fmt.Errorf("invalid fildId")
	}
	return str[0], str[1], nil
}
