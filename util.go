package tcp

import (
	"time"

	"github.com/dlshle/gommon/async"
	"github.com/dlshle/gommon/errors"
)

const ConnReadTimeoutErrMsg = "connection read timeout"

func ReadWithTimeout(executor async.Executor, duration time.Duration, conn Connection) (data []byte, err error) {
	readChan := make(chan bool, 1)
	executor.Execute(func() {
		data, err = conn.Read()
		readChan <- true
		close(readChan)
	})
	select {
	case <-time.NewTimer(duration).C:
		err = errors.Error(ConnReadTimeoutErrMsg)
		return
	case <-readChan:
		return
	}
}
