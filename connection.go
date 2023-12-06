package gts

import (
	"bufio"
	"context"
	gerrors "errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"

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
	OnMessage(func([]byte))
	Write([]byte) error
	Address() string
	OnError(func(error))
	OnClose(func(error))
	State() int
	ReadLoop()
	String() string
	IsLive() bool
	EnableLogging()
}

type TCPConnection struct {
	messageVersion      byte
	conn                net.Conn
	onMessageCb         func([]byte)
	onCloseCb           func(error)
	onErrorCb           func(error)
	state               int
	rwLock              *sync.RWMutex
	remainingReadBuffer []byte
	closeChan           chan bool
	debugLogging        bool
}

func NewTCPConnection(conn net.Conn) Connection {
	return &TCPConnection{
		conn:         conn,
		state:        StateIdle,
		rwLock:       new(sync.RWMutex),
		closeChan:    make(chan bool),
		debugLogging: false,
	}
}

func (c *TCPConnection) withWrite(cb func()) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	cb()
}

func (c *TCPConnection) setState(state int) {
	if state > 0 && state <= StateDisconnected {
		c.withWrite(func() {
			c.state = state
		})
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
	data, err := c.readV2()
	if err != nil {
		c.handleError(err)
	} else {
		c.handleMessage(data)
	}
	return data, err
}

func (c *TCPConnection) readV2() ([]byte, error) {
	c.log("reading...")
	var dataLength uint32
	r := bufio.NewReader(c.conn)
	// read header from message
	b := make([]byte, 6)
	n, err := io.ReadFull(r, b)
	if err != nil {
		if gerrors.Is(err, syscall.ECONNRESET) {
			c.log("reading header encountered ECONNRESET error: %s", err.Error())
			return nil, err
		}
		if err == io.EOF {
			c.log("reading header encountered EOF error: %s with n = %v", err.Error(), n)
			return c.readV2()
		}
		return nil, err
	}
	dataLength, err = computeFrameDataLength(c.messageVersion, b)
	if err != nil {
		return nil, err
	}
	c.log("message length %v", dataLength)
	contentBytes := make([]byte, dataLength)
	n, err = io.ReadFull(r, contentBytes)
	if err != nil {
		if gerrors.Is(err, syscall.ECONNRESET) {
			c.log("reading header encountered ECONNRESET error: %s", err.Error())
			return nil, err
		}
		if err == io.EOF {
			c.log("reading content for %v bytes encountered EOF error: %s with n = %v", dataLength, err.Error(), n)
			return c.readV2()
		}
		return nil, err
	}
	if n != int(dataLength) {
		c.log("read data length %v mismatch data length %v", n, dataLength)
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
	c.onMessageCb = cb
}

func (c *TCPConnection) handleMessage(message []byte) {
	if c.onMessageCb != nil {
		c.onMessageCb(message)
	}
}

func (c *TCPConnection) Write(data []byte) (err error) {
	wrappedData := wrapData(data, c.messageVersion)
	c.withWrite(func() {
		_, err = c.conn.Write(wrappedData)
	})
	if err != nil {
		c.handleError(err)
	}
	return err
}

func (c *TCPConnection) Address() string {
	return c.conn.RemoteAddr().String()
}

func (c *TCPConnection) OnError(cb func(err error)) {
	c.onErrorCb = cb
}

func (c *TCPConnection) handleError(err error) {
	if c.onErrorCb != nil {
		c.onErrorCb(err)
	} else {
		c.Close()
	}
}

func (c *TCPConnection) OnClose(cb func(err error)) {
	c.onCloseCb = cb
}

func (c *TCPConnection) handleClose(err error) {
	if c.onCloseCb != nil {
		c.onCloseCb(err)
	}
}

func (c *TCPConnection) State() int {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
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
			c.log("read message failed: %s", err.Error())
			c.onErrorCb(err)
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

func (c *TCPConnection) EnableLogging() {
	c.debugLogging = true
}

func (c *TCPConnection) log(formatter string, contents ...interface{}) {
	if !c.debugLogging {
		return
	}
	logging.GlobalLogger.Debugf(context.Background(), formatter, contents...)
}
