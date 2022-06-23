package tcp

import (
	"context"
	"fmt"
	"net"

	"github.com/dlshle/gommon/logger"
)

type TCPServer interface {
	Start() error
	Stop() error
	OnConnectionError(cb func(Connection, error))
	OnClientConnected(cb func(Connection))
	OnClientClosed(cb func(Connection, error))
	Address() string
}

type tcpServer struct {
	name     string
	address  string
	port     int
	logger   logger.Logger
	ctx      context.Context
	stopFunc func()

	onConnected     func(conn Connection)
	onDisconnected  func(conn Connection, err error)
	onConnectionErr func(conn Connection, err error)
}

func NewTCPServer(name, addr string, port int) TCPServer {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &tcpServer{
		name:     name,
		address:  addr,
		port:     port,
		logger:   logger.StdOutLevelLogger("[TCPServer]"),
		ctx:      ctx,
		stopFunc: cancelFunc,
	}
}

func (s *tcpServer) Start() error {
	s.logger.Info("starting TCP server...")
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.address, s.port))
	if err != nil {
		return err
	}
	select {
	case <-s.ctx.Done():
		s.logger.Info("stopping server ...")
		break
	default:
		conn, err := listener.Accept()
		if err != nil {
			s.logger.Info("listener err: " + err.Error())
			return err
		}
		go s.handleNewConnection(conn)
	}
	return listener.Close()
}

func (s *tcpServer) toTCPConnection(conn net.Conn) Connection {
	return NewTCPConnection(conn)
}

func (s *tcpServer) handleNewConnection(rawConn net.Conn) {
	conn := s.toTCPConnection(rawConn)
	s.logger.Info("new tcp connection ", conn.String())
	conn.OnError(func(err error) {
		s.onConnectionErr(conn, err)
	})
	conn.OnClose(func(err error) {
		s.onConnectionErr(conn, err)
	})
	s.onConnected(conn)
}

func (s *tcpServer) Stop() error {
	s.stopFunc()
	return nil
}

func (s *tcpServer) OnConnectionError(cb func(Connection, error)) {
	s.onConnectionErr = cb
}

func (s *tcpServer) OnClientConnected(cb func(Connection)) {
	s.onConnected = cb
}

func (s *tcpServer) OnClientClosed(cb func(Connection, error)) {
	s.onDisconnected = cb
}

func (s *tcpServer) Address() string {
	return s.address
}
