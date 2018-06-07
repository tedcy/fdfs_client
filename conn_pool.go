package fdfs_client

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	MAXCONNS_LEAST = 5
)

type pConn struct {
	net.Conn
	pool *connPool
}

func (c pConn) Close() error {
	return c.pool.put(c)
}

type connPool struct {
	conns    *list.List
	addr     string
	maxConns int
	count    int
	lock     *sync.RWMutex
	finish   chan bool
}

func newConnPool(addr string, maxConns int) (*connPool, error) {
	if maxConns < MAXCONNS_LEAST {
		return nil, fmt.Errorf("too little maxConns < %d", MAXCONNS_LEAST)
	}
	connPool := &connPool{
		conns:    list.New(),
		addr:     addr,
		maxConns: maxConns,
		lock:     &sync.RWMutex{},
		finish:   make(chan bool),
	}
	go func() {
		timer := time.NewTimer(time.Second * 20)
		for {
			select {
			case finish := <-connPool.finish:
				if finish {
					return
				}
			case <-timer.C:
				connPool.CheckConns()
				timer.Reset(time.Second * 20)
			}
		}
	}()
	connPool.lock.Lock()
	defer connPool.lock.Unlock()
	for i := 0; i < MAXCONNS_LEAST; i++ {
		if err := connPool.makeConn(); err != nil {
			return nil, err
		}
	}
	return connPool, nil
}

func (this *connPool) Destory() {
	if this == nil {
		return
	}
	this.finish <- true
}

func (this *connPool) CheckConns() error {
	this.lock.Lock()
	defer this.lock.Unlock()
	for e, next := this.conns.Front(), new(list.Element); e != nil; e = next {
		next = e.Next()
		conn := e.Value.(pConn)
		header := &header{
			cmd: FDFS_PROTO_CMD_ACTIVE_TEST,
		}
		if err := header.SendHeader(conn.Conn); err != nil {
			this.conns.Remove(e)
			this.count--
			continue
		}
		if err := header.RecvHeader(conn.Conn); err != nil {
			this.conns.Remove(e)
			this.count--
			continue
		}
		if header.cmd != TRACKER_PROTO_CMD_RESP || header.status != 0 {
			this.conns.Remove(e)
			this.count--
			continue
		}
	}
	return nil
}

func (this *connPool) makeConn() error {
	conn, err := net.DialTimeout("tcp", this.addr, time.Second*10)
	if err != nil {
		return err
	}
	this.conns.PushBack(pConn{
		Conn: conn,
		pool: this,
	})
	this.count++
	return nil
}

func (this *connPool) get() (net.Conn, error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	for {
		e := this.conns.Front()
		if e == nil {
			if this.count >= this.maxConns {
				return nil, fmt.Errorf("reach maxConns %d", this.maxConns)
			}
			this.makeConn()
			continue
		}
		this.conns.Remove(e)
		conn := e.Value.(pConn)
		return conn, nil
	}
	//not reach
	return nil, nil
}

func (this *connPool) put(pConn pConn) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	pConn.pool.conns.PushBack(pConn)
	return nil
}
