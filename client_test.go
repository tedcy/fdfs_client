package fdfs_client

import (
	"testing"
	"sync"
	"fmt"
)

func TestUpload(t *testing.T) {
	client, err := NewClientWithConfig("fdfs.conf")
	defer client.Destory()
	if err != nil {
		fmt.Println(err.Error())
		return
    }
	fileId, err := client.UploadByFilename("client_test.go")
	if err != nil {
		fmt.Println(err.Error())
		return
    }
	fmt.Println(fileId.GroupName + "/" + fileId.RemoteFileName)
	if err := client.DownloadByFileId(fileId.GroupName + "/" + fileId.RemoteFileName,"tempFile");err != nil {
		fmt.Println(err.Error())
		return
    }
	if err := client.DeleteByFileId(fileId.GroupName + "/" + fileId.RemoteFileName);err != nil {
		fmt.Println(err.Error())
		return
    }
}

func TestUpload100(t *testing.T) {
	client, err := NewClientWithConfig("fdfs.conf")
	defer client.Destory()
	if err != nil {
		fmt.Println(err.Error())
		return
    }
	var wg sync.WaitGroup
	for i := 0;i != 100;i++{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0;j != 10;j++ {
				if fileId, err := client.UploadByFilename("client_test.go");err != nil {
					fmt.Println(err.Error())
                } else {
					fmt.Println(fileId.GroupName + "/" + fileId.RemoteFileName)
					if err := client.DeleteByFileId(fileId.GroupName + "/" + fileId.RemoteFileName);err != nil {
						fmt.Println(err.Error())
					}
                }
            }
		}()
    }
	wg.Wait()	
}
