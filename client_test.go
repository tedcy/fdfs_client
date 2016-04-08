package fdfs_client

import (
	"testing"
	"sync"
	//"time"
	"fmt"
)

func TestUpload(t *testing.T) {
	client, err := NewClientWithConfig("fdfs.conf")
	if err != nil {
		t.Log(err.Error())
		return
    }
	fileId, err := client.UploadByFilename("client_test.go")
	if err != nil {
		t.Log(err.Error())
		return
    }
	t.Log(fileId.GroupName + "/" + fileId.RemoteFileName)
}

func TestUpload100(t *testing.T) {
	client, err := NewClientWithConfig("fdfs.conf")
	if err != nil {
		t.Log(err.Error())
		return
    }
	var wg sync.WaitGroup
	for i := 0;i != 100;i++{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0;j != 10;j++ {
				fileId, err := client.UploadByFilename("client_test.go")
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				fmt.Println(fileId.GroupName + "/" + fileId.RemoteFileName)
            }
			//time.Sleep(time.Second * 10)
		}()
    }
	wg.Wait()	
}
