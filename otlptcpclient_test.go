package otlptcpclient

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestExporter(t *testing.T) {
	svr := &testServer{}
	err := svr.start()
	require.NoError(t, err)

	defer svr.stop()

	exp, err := NewExporter(svr.listener.Addr().String())
	require.NoError(t, err)

	defer func() {
		_ = exp.Stop(context.Background())
	}()

	logs := logpb.ResourceLogs{
		ScopeLogs: []*logpb.ScopeLogs{
			{
				LogRecords: []*logpb.LogRecord{
					{
						TimeUnixNano: 1000,
					},
				},
			},
		},
	}

	err = exp.UploadLogs(context.Background(), &logs)
	require.NoError(t, err)

	logs.ScopeLogs[0].LogRecords[0].TimeUnixNano = 1001
	err = exp.UploadLogs(context.Background(), &logs)
	require.NoError(t, err)

	// force reconnect
	exp.disconnect()

	logs.ScopeLogs[0].LogRecords[0].TimeUnixNano = 1002
	err = exp.UploadLogs(context.Background(), &logs)
	require.NoError(t, err)

	logs.ScopeLogs[0].LogRecords[0].TimeUnixNano = 1003
	err = exp.UploadLogs(context.Background(), &logs)
	require.NoError(t, err)

	require.NoError(t, exp.Stop(context.Background()))

	svr.stop()

	require.Len(t, svr.messages, 4)

	for i := 0; i < 4; i++ {
		require.Equal(t, uint64(1000+i), svr.messages[i].ResourceLogs[0].ScopeLogs[0].LogRecords[0].TimeUnixNano)
	}
}

func (t *testServer) stop() {
	t.listener.Close()
	t.waitgroup.Wait()
}

func (t *testServer) start() error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	t.listener = listener

	t.waitgroup.Add(1)
	go func() {
		defer t.waitgroup.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			t.waitgroup.Add(1)
			go func() {
				defer t.waitgroup.Done()
				t.handleConn(conn)
			}()
		}
	}()

	return nil
}

func (t *testServer) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		prefix := []byte{0, 0, 0, 0, 0}

		if _, err := io.ReadFull(conn, prefix); err != nil {
			return
		}

		messageType := uint8(prefix[0])
		if messageType != messageTypeLog {
			return
		}

		length := binary.BigEndian.Uint32(prefix[1:])
		data := make([]byte, length)

		if _, err := io.ReadFull(conn, data); err != nil {
			return
		}

		var logData logpb.LogsData

		if err := proto.Unmarshal(data, &logData); err != nil {
			return
		}

		t.mu.Lock()
		t.messages = append(t.messages, &logData)
		t.mu.Unlock()
	}
}

type testServer struct {
	mu        sync.Mutex
	waitgroup sync.WaitGroup
	listener  net.Listener
	messages  []*logpb.LogsData
}
