package gts

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/dlshle/gommon/logging"
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
	logger   logging.Logger
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
		logger:   logging.GlobalLogger.WithPrefix("[TCPServer]"),
		ctx:      ctx,
		stopFunc: cancelFunc,
	}
}

func (s *tcpServer) Start() error {
	if s.onConnected == nil {
		s.logger.Errorf(s.ctx, "onConnected callback is not set")
		return errors.New("onConnected callback is not set")
	}
	s.logger.Info(s.ctx, "starting TCP server...")
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.address, s.port))
	if err != nil {
		return err
	}
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info(s.ctx, "stopping server ...")
			return listener.Close()
		default:
			conn, err := listener.Accept()
			if err != nil {
				s.logger.Info(s.ctx, "listener err: "+err.Error())
				return err
			}
			go s.handleNewConnection(conn)
		}
	}
}

func (s *tcpServer) toTCPConnection(conn net.Conn) Connection {
	return NewTCPConnection(conn)
}

func (s *tcpServer) handleNewConnection(rawConn net.Conn) {
	conn := s.toTCPConnection(rawConn)
	s.logger.Info(s.ctx, "new tcp connection ", conn.String())
	conn.OnError(func(err error) {
		if s.onConnectionErr != nil {
			s.onConnectionErr(conn, err)
		}
	})
	conn.OnClose(func(err error) {
		if s.onConnectionErr != nil {
			s.onConnectionErr(conn, err)
		}
	})
	if s.onConnected != nil {
		s.onConnected(conn)
	}
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
