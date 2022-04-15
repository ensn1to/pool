package pool

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type PoolConfig struct {
	InitialCap  int // inital capacity
	MaxCap      int // max capacity
	Factory     Factory
	IdleTimeout time.Duration
	WaitTimeout time.Duration
}

// channelPool implements the Pool interface based on buffered channels.
type channelPool struct {
	// storage for our net.Conn connections
	mu    sync.RWMutex
	conns chan *idleConn

	// net.Conn generator
	factory Factory

	idleTimeout time.Duration
	waitTimeout time.Duration
}

type idleConn struct {
	conn net.Conn
	t    time.Time
}

// Factory is a function to create new connections.
type Factory func() (net.Conn, error)

// NewChannelPool returns a new pool based on buffered channels with an initial
// capacity and maximum capacity. Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool, a new connection will be created via the Factory()
// method.
func NewChannelPool(config *PoolConfig) (Pool, error) {
	if config.InitialCap < 0 || config.MaxCap <= 0 || config.InitialCap > config.MaxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &channelPool{
		conns:       make(chan *idleConn, config.MaxCap),
		factory:     config.Factory,
		idleTimeout: config.IdleTimeout,
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < config.InitialCap; i++ {
		conn, err := config.Factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- &idleConn{conn: conn, t: time.Now()}
	}

	return c, nil
}

func (c *channelPool) getConnsAndFactory() (chan *idleConn, Factory) {
	c.mu.RLock()
	conns := c.conns
	factory := c.factory
	c.mu.RUnlock()
	return conns, factory
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *channelPool) Get() (net.Conn, error) {
	conns, factory := c.getConnsAndFactory()

	// todo: test if it is ok like this
	if conns == nil {
		fmt.Println("Get, conns is nil")
		return nil, ErrClosed
	}

	// wrap our connections with out custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	// TRY:
	select {
	// case <-time.After(c.waitTimeout):
	// 	goto TRY
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}

		return c.wrapConn(conn.conn), nil
	default:
		conn, err := factory()
		if err != nil {
			return nil, err
		}

		return c.wrapConn(conn), nil
	}
}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *channelPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- &idleConn{conn: conn, t: time.Now()}:
		return nil
	default:
		// pool is full, close passed connection
		return conn.Close()
	}
}

func (c *channelPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.conn.Close()
	}
}

func (c *channelPool) Len() int {
	conns, _ := c.getConnsAndFactory()
	return len(conns)
}
