<center>
<h2>fastDfs client</h2>
</center>

## Introduction
fdfs_client is a useful tool for go projects to upload/download/delete files from fast dfs service.

## Support
```
//upload
upload(UploadByFilename,UploadByBuffer)

//download
download(DownloadToFile,DownloadToBuffer)

//delete
delete(DeleteFile)
```

## Notice
1 UploadByFilename realized with sendfile syscall in linux,so UploadByBuffer is depracated<br/>
2 realized conn_pool,pool_size control by config file<br/>
3 you can just pass string array hosts to new the client

## Usage
```
$ go get github.com/HelloMrShu/fdfs_client
```
## Licence
<a href="https://zhuanlan.zhihu.com/p/350968635">MIT</a>

## Author
turbo

