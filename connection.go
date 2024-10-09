package gts

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/dlshle/gommon/errors"
	"github.com/dlshle/gommon/logging"
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
	ReadV1(bufferSize int) ([]byte, error)
	OnMessage(func([]byte))
	Write([]byte) error
	Address() string
	OnError(func(error))
	OnClose(func(error))
	State() int
	ReadLoop()
	String() string
	IsLive() bool
	Verbose(bool)
}

type TCPConnection struct {
	messageVersion      byte
	conn                net.Conn
	r                   io.Reader
	onMessage           func([]byte)
	onClose             func(error)
	onError             func(error)
	state               int
	remainingReadBuffer []byte
	closeChan           chan bool
	mutex               *sync.Mutex
	verbose             bool
}

func NewTCPConnection(conn net.Conn) Connection {
	return &TCPConnection{
		conn:      conn,
		r:         bufio.NewReader(conn),
		state:     StateIdle,
		closeChan: make(chan bool),
		mutex:     new(sync.Mutex),
		verbose:   false,
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

func (c *TCPConnection) ReadV1(bufferSize int) ([]byte, error) {
	if c.state != StateIdle {
		return nil, errors.Error("invalid state for synchronous reading")
	}
	if bufferSize == 0 {
		bufferSize = DefaultReadBufferSize
	}
	data, err := c.read(bufferSize)
	if err != nil {
		c.handleError(err)
	} else {
		c.handleMessage(data)
	}
	return data, err
}

func (c *TCPConnection) Read() ([]byte, error) {
	if c.state != StateIdle {
		return nil, errors.Error("invalid state for synchronous reading")
	}
	data, err := c.readV2()
	if err != nil {
		c.handleError(err)
	} else {
		c.handleMessage(data)
	}
	return data, err
}

func (c *TCPConnection) readV2() ([]byte, error) {
	var dataLength uint32
	// read header from message
	b := make([]byte, headerLength)
	n, err := io.ReadFull(c.r, b)
	c.maybeLog("read header from stream with %d bytes, err = %v", n, err)
	if err != nil {
		return nil, err
	}
	dataLength, err = computeFrameDataLength(c.messageVersion, b)
	c.maybeLog("from header, data length = %d bytes, err = %v ", dataLength, err)
	if err != nil {
		return nil, err
	}
	contentBytes := make([]byte, dataLength)
	n, err = io.ReadFull(c.r, contentBytes)
	c.maybeLog("read %d bytes for subsequent data, data length = %d bytes, err = %v", n, dataLength, err)
	if err != nil {
		return nil, err
	}
	if n != int(dataLength) {
		return nil, errors.Error("failed to read expected data: incorrect bytes read from stream")
	}
	return contentBytes, nil
}

func (c *TCPConnection) read(bufferSize int) ([]byte, error) {
	var dataLength, consumedLength uint32
	buffer := make([]byte, bufferSize)
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
	if dataLength <= uint32(bufferSize)-headerLength {
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
	n, err := c.conn.Write(wrappedData)
	c.maybeLog("wrote %d bytes to stream, data length = %d bytes, err = %v", n, len(wrappedData), err)
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
		msg, err := c.readV2()
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

func (c *TCPConnection) Verbose(verbose bool) {
	c.verbose = verbose
}

func (c *TCPConnection) maybeLog(format string, args ...interface{}) {
	if c.verbose {
		logging.GlobalLogger.Debugf(context.Background(), format, args...)
	}
}
