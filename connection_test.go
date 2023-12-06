package gts

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/dlshle/gommon/logging"
)

func TestCSBasicCommunication(t *testing.T) {
	someVeryLargeString := "hello world!"
	s := NewTCPServer("server", "localhost", 14514)
	s.OnClientConnected(func(conn Connection) {
		t.Logf("client connected: %s\n", conn.Address())
		go conn.ReadLoop()
		conn.OnMessage(func(b []byte) {
			logging.GlobalLogger.Debugf(context.Background(), "server recv: %d", len(b))
			conn.Write(append([]byte("server reply:"), b...))
		})
	})
	go func() {
		if err := s.Start(); err != nil {
			panic(err)
		}
	}()

	c := NewTCPClient("localhost", 14514)
	c.OnConnectionEstablished(func(conn Connection) {
		go conn.ReadLoop()
		conn.OnMessage(func(b []byte) {
			logging.GlobalLogger.Debugf(context.Background(), "client recv: %d", len(b))
		})
		err := conn.Write([]byte("hello world!"))
		if err != nil {
			t.Errorf("failed to write to server: %s\n", err.Error())
			t.FailNow()
		}
		for i := 0; i < 1000; i++ {
			someVeryLargeString += "hello world!"
		}
		err = conn.Write([]byte(someVeryLargeString))
		if err != nil {
			t.Errorf("failed to write to server: %s\n", err.Error())
			t.FailNow()
		}
		someVeryLargeString += someVeryLargeString
		err = conn.Write([]byte(someVeryLargeString))
		if err != nil {
			t.Errorf("failed to write to server: %s\n", err.Error())
			t.FailNow()
		}
		someVeryLargeString += someVeryLargeString
		err = conn.Write([]byte(someVeryLargeString))
		if err != nil {
			t.Errorf("failed to write to server: %s\n", err.Error())
			t.FailNow()
		}
		someVeryLargeString += someVeryLargeString
		err = conn.Write([]byte(someVeryLargeString))
		if err != nil {
			t.Errorf("failed to write to server: %s\n", err.Error())
			t.FailNow()
		}
		specialContent := make([]byte, headerLength+2)
		binary.BigEndian.PutUint32(specialContent, 16)
		specialContent[4] = 2
		specialContent[5] = 12
		specialContent[6] = 0
		specialContent[7] = 1

		err = conn.Write(specialContent)
		if err != nil {
			t.Errorf("failed to write to server: %s\n", err.Error())
			t.FailNow()
		}

	})

	_, err := c.Connect("")
	if err != nil {
		t.Errorf("failed to connect to server: %s\n", err.Error())
		t.FailNow()
	}

	time.Sleep(5 * time.Second)
	t.FailNow()
}
