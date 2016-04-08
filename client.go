package fdfs_client

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

type Client struct {
	trackerPools	map[string]*ConnPool
	storagePools	map[string]*ConnPool
	storagePoolLock *sync.RWMutex
	config			*Config
}

func NewClientWithConfig(configName string) (*Client, error) {
	config, err := NewConfig(configName)
	if err != nil {
		return nil, err
    }
	client := &Client{
		config:					config,
		storagePoolLock:		&sync.RWMutex{},
	}
	client.trackerPools = make(map[string]*ConnPool)
	client.storagePools = make(map[string]*ConnPool)

	for _, addr := range config.trackerAddr {
		trackerPool, err := NewConnPool(addr, config.maxConns)
		if err != nil {
			return nil, err
		}
		client.trackerPools[addr] = trackerPool
    }

	return client, nil
}

func (this *Client) QueryStorageInfoWithTracker(conn net.Conn) (*StorageInfo, error) {
	task := &TrackerUploadTask{}
	if err := task.SendHeader(conn); err != nil {
		return nil, err
	}
	if err := task.RecvHeader(conn); err != nil {
		return nil, err
	}

	if task.status != 0 {
		return nil, fmt.Errorf("tracker task status %v != 0", task.status)
	}
	if task.pkgLen != 40 {
		return nil, fmt.Errorf("tracker task pkgLen %v != 0", task.status)
	}
	if err := task.RecvBody(conn); err != nil {
		return nil, err
	}
	return &StorageInfo{
		addr:             fmt.Sprintf("%s:%d", task.ipAddr, task.port),
		storagePathIndex: task.storePathIndex,
	}, nil
}

func (this *Client) QueryFileInfo(fileName string) (*FileInfo, error) {
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
	return &FileInfo{
		fileSize:    stat.Size(),
		file:        file,
		fileExtName: fileExtName,
	}, nil
}

func (this *Client) UploadFileToStorage(fileInfo *FileInfo, storageInfo *StorageInfo) (*FileId, error) {
	storageConn, err := this.GetStorageConn(storageInfo)
	if err != nil {
		return nil, err
	}
	defer storageConn.Close()

	task := &StorageUploadTask{}
	err = task.SendHeader(storageConn, fileInfo, storageInfo.storagePathIndex)
	if err != nil {
		return nil, err
	}
	err = task.SendFile(storageConn, fileInfo)
	if err != nil {
		return nil, err
	}
	return task.RecvFileId(storageConn)
}

func (this *Client) UploadByFilename(fileName string) (*FileId, error) {
	fileInfo, err := this.QueryFileInfo(fileName)
	defer func() {
		if fileInfo != nil && fileInfo.file != nil{
			fileInfo.file.Close()
        }
	}()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	trackerConn, err := this.GetTrackerConn()
	if err != nil {
		return nil, err
	}
	defer func(){
		trackerConn.Close()
	}()

	storageInfo, err := this.QueryStorageInfoWithTracker(trackerConn)
	if err != nil {
		return nil, err
	}

	return this.UploadFileToStorage(fileInfo, storageInfo)
}

func (this *Client) GetTrackerConn() (net.Conn, error) {
	var trackerConn net.Conn
	var err error
	var getOne bool
	for _, trackerPool := range this.trackerPools {
		trackerConn, err = trackerPool.get()
		if err == nil {
			getOne = true
			break
		}
	}
	if getOne {
		return trackerConn, nil
	}
	if err == nil {
		return nil, fmt.Errorf("no connPool can be use")
	}
	return nil, err
}

func (this *Client) GetStorageConn(storageInfo *StorageInfo) (net.Conn, error) {
	this.storagePoolLock.Lock()
	storagePool, ok := this.storagePools[storageInfo.addr]
	if ok {
		this.storagePoolLock.Unlock()
		return storagePool.get()
	}
	fmt.Println("NewConnPool",storageInfo.addr,this,storagePool,ok)
	storagePool, err := NewConnPool(storageInfo.addr,this.config.maxConns)
	if err != nil {
		this.storagePoolLock.Unlock()
		return nil, err
	}
	this.storagePools[storageInfo.addr] = storagePool
	this.storagePoolLock.Unlock()
	return storagePool.get()
}
