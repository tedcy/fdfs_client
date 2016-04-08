package fdfs_client

import (
	"bufio"
	"io"
	"os"
	"strings"
	"strconv"
)

type Config struct {
	trackerAddr			[]string
	maxConns			int
}

func NewConfig(configName string) (*Config, error) {
	config := &Config{}
	f, err := os.Open(configName)
	if err != nil {
		return nil, err 
	}
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		line = strings.TrimSuffix(line, "\n")
		str := strings.SplitN(line, "=", 2)
		switch str[0] {
		case "tracker_server":
			config.trackerAddr = append(config.trackerAddr, str[1])
		case "maxConns":
			config.maxConns,err = strconv.Atoi(str[1])
			if err != nil {
				return nil, err
            }
		}
		if err != nil {
			if err == io.EOF {
				return config, nil
			}
			return nil, err
		}
	}
	return config, nil
}
