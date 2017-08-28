package dynamodb_local_server

import (
	"testing"
	"time"
)

func TestStart(t *testing.T) {
	s := Start("48791")
	time.Sleep(time.Second * 2)
	s.Stop()
}
