package dynamodb_local_server

import (
	"archive/zip"
	"bufio"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"time"
)

const (
	DynamoDBLocal_Jar_Path = "dynamodb-local/DynamoDBLocal.jar"
	DynamoDBLocal_Zip_Name = "dynamodb_local_latest.zip"
)
const (
	DynamoDBLocal_DownloadJarUrl = "https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.zip"
)

var currentDir string

type DynamoDBServer struct {
	cmd     *exec.Cmd
	done    chan struct{}
}

func init() {
	// current caller
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("No caller information")
	}
	currentDir = path.Dir(filename)
}

// Start a local DynamoDB server
// If no local jar found, will download the executable from aws
func Start(port string) *DynamoDBServer {
	if _, err := os.Stat(path.Join(currentDir, DynamoDBLocal_Jar_Path)); os.IsNotExist(err) {
		log.Println("No DynamoDBLocal lib found. Downloading it for testing purpose")
		downloadLocalLib()
	}

	return startServer(port)
}

func extractAndWriteFile(dest string, f *zip.File) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			panic(err)
		}
	}()

	filepath := path.Join(dest, f.Name)

	if f.FileInfo().IsDir() {
		os.MkdirAll(filepath, f.Mode())
	} else {
		os.MkdirAll(path.Dir(filepath), f.Mode())
		f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}
		defer func() {
			if err := f.Close(); err != nil {
				panic(err)
			}
		}()

		_, err = io.Copy(f, rc)
		if err != nil {
			return err
		}
	}
	return nil
}

func bytesToMb(b int64) float64 {
	return float64(b) / float64(1024*1024)
}

func unzipLib(zipname string) error {
	// unzip
	log.Println("Unzipping files...")
	r, err := zip.OpenReader(zipname)
	if err != nil {
		return err
	}
	defer r.Close()

	dest := path.Join(path.Dir(path.Join(currentDir, DynamoDBLocal_Jar_Path)))
	for _, f := range r.File {
		err := extractAndWriteFile(dest, f)
		if err != nil {
			return err
		}
	}

	return nil
}

func downloadZip(zipname string) {
	out, err := os.Create(zipname)
	if err != nil {
		panic(err)
	}
	defer out.Close()

	log.Printf("Downloading from %s\n", DynamoDBLocal_DownloadJarUrl)
	req, _ := http.NewRequest("GET", DynamoDBLocal_DownloadJarUrl, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	log.Printf("Zip file size: %6.2fM\n", bytesToMb(resp.ContentLength))

	log.Printf("Write zip file to %s\n", zipname)
	go func() {
		for {
			// monitoring file size
			f, err := os.Stat(zipname)
			if err != nil {
				log.Print(err)
				continue
			}
			log.Printf("Downloading... %6.2fM/%6.2fM \n", bytesToMb(f.Size()), bytesToMb(resp.ContentLength))
			time.Sleep(time.Second * 2)
		}
	}()
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		panic(err)
	}
}

func downloadLocalLib() {
	zipname := path.Join(currentDir, DynamoDBLocal_Zip_Name)

	if _, err := os.Stat(zipname); os.IsNotExist(err) {
		downloadZip(zipname)
	} else {
		log.Printf("%s already exists. Try to unzip it.", zipname)
	}

	if err := unzipLib(zipname); err != nil {
		panic(err)
	}
}

func startServer(port string) *DynamoDBServer {
	// StartServerCmd = "java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -inMemory -port xxxx"
	s := &DynamoDBServer{
		cmd: exec.Command(
			"java",
			"-jar", path.Join(currentDir, DynamoDBLocal_Jar_Path),
			"-sharedDb",
			"-inMemory",
			"-port", port),
		done: make(chan struct{}),
	}

	// stdout writer
	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	s.cmd.Stderr = os.Stderr
	if err := s.cmd.Start(); err != nil {
		log.Fatal(err)
	}

	ready := make(chan error, 1)
	go s.watch(stdout, ready)
	select {
	case err := <-ready:
		if err == nil {
			log.Printf("DynamoDB local server running on pid %d, port %s\n", s.cmd.Process.Pid, port)
			break
		}
	case <-time.After(time.Second * 10):
		err = errors.New("timeout waiting for server to start")
	}

	return s
}

func (s *DynamoDBServer) watch(stdout io.Reader, ready chan error) {
	scnout := bufio.NewScanner(stdout)

	for scnout.Scan() {
		text := scnout.Text()
		log.Println(text)
		if strings.Contains(text, "Initializing DynamoDB Local with the following configuratio") {
			ready <- nil
		}
	}

	s.cmd.Wait()
	log.Println("DynamoDB local server stopped")
	close(s.done)
}

func (s *DynamoDBServer) Stop() {
	log.Println("Stopping DynamoDB local server")
	s.cmd.Process.Signal(os.Interrupt)
	<-s.done
}
