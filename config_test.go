package fdfs_client

import (
	"testing"
	"fmt"
)

func TestConfig(t *testing.T) {
	config, err := newConfig("fdfs.conf")
	if err != nil {
		fmt.Println(err)
		return;
	}
	fmt.Println(config.trackerAddr)
	fmt.Println(config.maxConns)
}
