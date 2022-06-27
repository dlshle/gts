package gts

import (
	"fmt"
	"net"

	"github.com/dlshle/gommon/errors"

	"github.com/dlshle/gommon/logger"
)

type TCPClient interface {
	Connect(token string) (Connection, error)
	Disconnect() error
	Connection() Connection
	OnConnectionEstablished(cb func(conn Connection))
	OnDisconnected(cb func(error))
	OnError(cb func(error))
}

const DefaultRetryCount = 3

type tcpClient struct {
	serverAddr      string
	serverPort      int
	retryCount      int
	logger          logger.Logger
	onConnected     func(conn Connection)
	onMessage       func([]byte)
	onDisconnected  func(err error)
	onConnectionErr func(err error)
	conn            Connection
}

func NewTCPClient(serverAddr string, serverPort int) TCPClient {
	return &tcpClient{
		serverAddr: serverAddr,
		serverPort: serverPort,
		retryCount: DefaultRetryCount,
		logger:     logger.StdOutLevelLogger("[TCPClient]"),
	}
}

func (c *tcpClient) Connect(token string) (Connection, error) {
	return c.connectWithRetry(c.retryCount, nil)
}

func (c *tcpClient) connectWithRetry(retry int, lastErr error) (Connection, error) {
	if retry == 0 {
		return nil, lastErr
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", c.serverAddr, c.serverPort))
	if err != nil {
		return c.connectWithRetry(retry-1, err)
	}
	return c.handleConnection(conn)
}

func (c *tcpClient) handleConnection(rawConn net.Conn) (Connection, error) {
	conn := NewTCPConnection(rawConn)
	conn.OnError(c.onConnectionErr)
	conn.OnClose(c.onDisconnected)
	conn.OnMessage(c.onMessage)
	c.onConnected(conn)
	return conn, nil
}

func (c *tcpClient) ReadLoop() {
	if c.conn != nil {
		c.conn.ReadLoop()
	}
}

func (c *tcpClient) Disconnect() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return errors.Error("no connection has been established yet")
}

func (c *tcpClient) Write(data []byte) error {
	if c.conn != nil {
		return c.conn.Write(data)
	}
	return errors.Error("no connection has been established yet")
}

func (c *tcpClient) Read() ([]byte, error) {
	if c.conn != nil {
		if c.conn.State() < StateReading {
			return c.conn.Read()
		}
		return nil, errors.Error("connection is in reading loop")
	}
	return nil, errors.Error("no connection has been established yet")
}

func (c *tcpClient) Connection() Connection {
	return c.conn
}

func (c *tcpClient) OnConnectionEstablished(cb func(conn Connection)) {
	c.onConnected = cb
}

func (c *tcpClient) OnDisconnected(cb func(error)) {
	c.onDisconnected = cb
}

func (c *tcpClient) OnMessage(cb func([]byte)) {
	c.onMessage = cb
}

func (c *tcpClient) OnError(cb func(error)) {
	c.onConnectionErr = cb
}
