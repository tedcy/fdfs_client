package fdfs_client

import (
	"os"
)

type FileId struct {
	GroupName		string
	RemoteFileName  string
}

type StorageInfo struct {
	addr             string
	storagePathIndex int8
}

type FileInfo struct {
	fileSize    int64
	file        *os.File
	fileExtName string
}
