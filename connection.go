package gts

import (
	"bufio"
	gerrors "errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"

	"github.com/dlshle/gommon/errors"
)

const (
	DefaultReadBufferSize = 512
	StateIdle             = 0
	StateReading          = 1
	StateStopping         = 2
	StateStopped          = 3
	StateClosing          = 4
	StateDisconnected     = 5
)

type Connection interface {
	Close() error
	Read() ([]byte, error)
	OnMessage(func([]byte))
	Write([]byte) error
	Address() string
	OnError(func(error))
	OnClose(func(error))
	State() int
	ReadLoop()
	String() string
	IsLive() bool
}

type TCPConnection struct {
	messageVersion      byte
	conn                net.Conn
	onMessage           func([]byte)
	onClose             func(error)
	onError             func(error)
	state               int
	remainingReadBuffer []byte
	closeChan           chan bool
	mutex               *sync.Mutex
}

func NewTCPConnection(conn net.Conn) Connection {
	return &TCPConnection{
		conn:      conn,
		state:     StateIdle,
		closeChan: make(chan bool),
		mutex:     new(sync.Mutex),
	}
}

func (c *TCPConnection) setState(state int) {
	if state > 0 && state <= StateDisconnected {
		c.state = state
	}
}

func (c *TCPConnection) Close() error {
	if c.State() >= StateClosing {
		return errors.Error("err: closing a closing connection")
	}
	c.setState(StateClosing)
	err := c.conn.Close()
	c.handleClose(err)
	c.setState(StateDisconnected)
	return err
}

func (c *TCPConnection) Read() ([]byte, error) {
	if c.state != StateIdle {
		return nil, errors.Error("invalid state for synchronous reading")
	}
	data, err := c.read()
	if err != nil {
		c.handleError(err)
	} else {
		c.handleMessage(data)
	}
	return data, err
}

func (c *TCPConnection) readV2(r *bufio.Reader) ([]byte, error) {
	var dataLength uint32
	if r == nil {
		r = bufio.NewReader(c.conn)
	}
	// read header from message
	b := make([]byte, 6)
	n, err := io.ReadFull(r, b)
	if err != nil {
		if gerrors.Is(err, syscall.ECONNRESET) {
			return nil, err
		}
		if err == io.EOF {
			return c.readV2(r)
		}
		if n != 6 {
			return nil, err
		}
		return nil, err
	}
	dataLength, err = computeFrameDataLength(c.messageVersion, b)
	if err != nil {
		return nil, err
	}
	contentBytes := make([]byte, dataLength)
	n, err = io.ReadFull(r, contentBytes)
	if err != nil {
		if gerrors.Is(err, syscall.ECONNRESET) {
			return nil, err
		}
		if err == io.EOF {
			return c.readV2(r)
		}
		if n != int(dataLength) {
			return nil, err
		}
		return nil, err
	}
	if n != int(dataLength) {
		return nil, errors.Error("failed to read expected data: incorrect bytes read from stream")
	}
	return contentBytes, nil
}

func (c *TCPConnection) read() ([]byte, error) {
	var dataLength, consumedLength uint32
	buffer := make([]byte, DefaultReadBufferSize)
	n, err := c.conn.Read(buffer)
	if err != nil {
		return nil, err
	}
	if c.remainingReadBuffer != nil {
		buffer = append(c.remainingReadBuffer, buffer[:n]...)
		n = len(buffer)
		c.remainingReadBuffer = nil
	}
	dataLength, err = computeFrameDataLength(c.messageVersion, buffer[:headerLength])
	if err != nil {
		return nil, err
	}
	if headerLength >= n {
		return nil, errors.Error("invalid buffer length, buffer length is expected to be larger than header size")
	}
	buffer = buffer[headerLength:n]
	if dataLength <= DefaultReadBufferSize-headerLength {
		// put the remaining stream into the remaining read buffer
		c.remainingReadBuffer = buffer[dataLength:]
		return buffer[:dataLength], nil
	}
	consumedLength = uint32(n - headerLength)
	// continue reading rest of the message data
	for consumedLength < dataLength {
		tempBuffer := make([]byte, dataLength-consumedLength)
		n, err = c.conn.Read(tempBuffer)
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, tempBuffer[:n]...)
		consumedLength += uint32(n)
	}
	// put the remaining stream into the remaining read buffer
	c.remainingReadBuffer = buffer[dataLength:]
	return buffer, nil
}

func (c *TCPConnection) OnMessage(cb func([]byte)) {
	c.onMessage = cb
}

func (c *TCPConnection) handleMessage(message []byte) {
	if c.onMessage != nil {
		c.onMessage(message)
	}
}

func (c *TCPConnection) Write(data []byte) (err error) {
	wrappedData := wrapData(data, c.messageVersion)
	c.mutex.Lock()
	_, err = c.conn.Write(wrappedData)
	c.mutex.Unlock()
	if err != nil {
		c.handleError(err)
	}
	return err
}

func (c *TCPConnection) Address() string {
	return c.conn.RemoteAddr().String()
}

func (c *TCPConnection) OnError(cb func(err error)) {
	c.onError = cb
}

func (c *TCPConnection) handleError(err error) {
	if c.onError != nil {
		c.onError(err)
	} else {
		c.Close()
	}
}

func (c *TCPConnection) OnClose(cb func(err error)) {
	c.onClose = cb
}

func (c *TCPConnection) handleClose(err error) {
	if c.onClose != nil {
		c.onClose(err)
	}
}

func (c *TCPConnection) State() int {
	return c.state
}

func (c *TCPConnection) ReadLoop() {
	if c.State() > StateIdle {
		return
	}
	c.setState(StateReading)
	// c.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	for c.State() == StateReading {
		// r := bufio.NewReader(c.conn)
		msg, err := c.read()
		if err == nil {
			c.handleMessage(msg)
		} else if err != nil {
			c.onError(err)
			break
		}
	}
	c.setState(StateStopped)
	close(c.closeChan)
}

func (c *TCPConnection) String() string {
	return fmt.Sprintf(`{"address": "%s","state": %d }`, c.Address(), c.State())
}

func (c *TCPConnection) IsLive() bool {
	return c.State() == StateReading
}
