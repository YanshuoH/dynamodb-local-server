# dynamodb-local-server
Create a local DynamoDB server for testing purpose on runtime

# Run
Simply include the package and start with defined port.
Eg.
```go
package main
import (
	"time"
	server "github.com/YanshuoH/dynamodb-local-server"
)

func main() {
	ss := server.Start("48791")
	// do whatever you want
	time.Sleep(time.Second)
	ss.Stop()
}
```

# Test
`go test`