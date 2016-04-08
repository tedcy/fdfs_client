package fdfs_client

import (
	"bytes"
)

func ReadCStrFromByteBuffer(buffer *bytes.Buffer, size int) (string, error) {
	buf := make([]byte, size)
	if _, err := buffer.Read(buf); err != nil {
		return "", err
	}

	index := bytes.IndexByte(buf, 0x00)

	if index == -1 {
		return string(buf), nil
	}

	return string(buf[0:index]), nil
}
