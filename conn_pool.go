package fdfs_client

import (
	"net"
	"time"
	"fmt"
	"sync"
	//"time"
	"container/list"
)

const (
	MAXCONNS_LEAST	=	5
)

type pConn struct {
	net.Conn
	pool			*ConnPool	
}

func (c pConn) Close() error {
	return c.pool.put(c)
}

type ConnPool struct {
	conns           *list.List
	addr			string
	maxConns		int
	count			int
	lock            *sync.RWMutex
}

func NewConnPool(addr string,maxConns int) (*ConnPool, error) {
	if maxConns < MAXCONNS_LEAST {
		return nil,fmt.Errorf("too little maxConns < %d",MAXCONNS_LEAST)
    }
	if maxConns < 1 {
		return nil,fmt.Errorf("too little maxConns < 1")
    }
	connPool := &ConnPool{
		conns:			list.New(),
		addr:			addr,
		maxConns:		maxConns,
		lock:			&sync.RWMutex{},
	}
	connPool.lock.Lock()
	defer connPool.lock.Unlock()
	for i := 0;i < MAXCONNS_LEAST;i++ {
		connPool.makeConn()	
    }
	return connPool, nil
}

func (this *ConnPool) CheckConns() error{
	return nil
}

func (this *ConnPool) makeConn() error{
	conn, err := net.DialTimeout("tcp", this.addr, time.Minute)
	if err != nil {
		return err
	}
	this.conns.PushBack(pConn{
		Conn:		conn,
		pool:		this,
	})
	this.count++
	return nil
}

func (this *ConnPool) get() (net.Conn,error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	for {
		e := this.conns.Front()
		if e == nil {
			if this.count >= this.maxConns {
				return nil, fmt.Errorf("reach maxConns %d",this.maxConns)
            }
			this.makeConn()
			continue
        }
		this.conns.Remove(e)
		conn := e.Value.(pConn)
		return conn, nil	
    }
	//not reach
	return nil,nil
}

func (this *ConnPool) put(pConn pConn) error{
	this.lock.Lock()
	defer this.lock.Unlock()
	pConn.pool.conns.PushBack(pConn)
	return nil
}
