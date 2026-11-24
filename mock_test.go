package nsq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) logger {
	return &testLogger{tbl}
}

type instruction struct {
	delay     time.Duration
	frameType int32
	body      []byte
}

type mockNSQD struct {
	t                 *testing.T
	script            []instruction
	got               [][]byte
	gotMtx            sync.Mutex
	tcpAddr           *net.TCPAddr
	tcpListener       net.Listener
	exitChan          chan int
	wg                sync.WaitGroup
	listenerCloseOnce sync.Once
	// when true, CLOSE_WAIT is acknowledged but the server side of the
	// connection stays open until the mock shuts down (mirrors nsqd waiting
	// for in-flight handlers to drain)
	holdAfterCloseWait bool
}

func newMockNSQD(t *testing.T, script []instruction, addr string) *mockNSQD {
	return newMockNSQDOpts(t, script, addr, false)
}

func newMockNSQDOpts(t *testing.T, script []instruction, addr string, holdAfterCloseWait bool) *mockNSQD {
	n := &mockNSQD{
		t:                  t,
		script:             script,
		exitChan:           make(chan int),
		holdAfterCloseWait: holdAfterCloseWait,
	}

	tcpListener, err := net.Listen("tcp", addr)
	if err != nil {
		n.t.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr, err)
	}
	n.tcpListener = tcpListener
	n.tcpAddr = tcpListener.Addr().(*net.TCPAddr)

	n.wg.Add(1)
	go n.listen()

	// ensure the listener is shut and every mock goroutine has exited before
	// the test completes, so detached goroutines can not log past teardown
	t.Cleanup(func() {
		n.closeListener()
		n.wg.Wait()
	})

	return n
}

// closeListener shuts the accept loop down; safe to call more than once.
func (n *mockNSQD) closeListener() {
	n.listenerCloseOnce.Do(func() {
		n.tcpListener.Close()
	})
}

func (n *mockNSQD) listen() {
	defer n.wg.Done()
	n.t.Logf("TCP: listening on %s", n.tcpListener.Addr())

	for {
		conn, err := n.tcpListener.Accept()
		if err != nil {
			break
		}
		n.wg.Add(1)
		go n.handle(conn)
	}

	n.t.Logf("TCP: closing %s", n.tcpListener.Addr())
	close(n.exitChan)
}

func (n *mockNSQD) handle(conn net.Conn) {
	defer n.wg.Done()
	var idx int

	n.t.Logf("TCP: new client(%s)", conn.RemoteAddr())

	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		n.t.Fatalf("ERROR: failed to read protocol version - %s", err)
	}

	readChan := make(chan []byte)
	readDoneChan := make(chan int)
	scriptTime := time.After(n.script[0].delay)
	rdr := bufio.NewReader(conn)

	go func() {
		for {
			line, err := rdr.ReadBytes('\n')
			if err != nil {
				return
			}
			// trim the '\n'
			line = line[:len(line)-1]
			readChan <- line
			<-readDoneChan
		}
	}()

	var rdyCount int
	for idx < len(n.script) {
		select {
		case line := <-readChan:
			n.t.Logf("mock: %s", line)
			n.gotMtx.Lock()
			n.got = append(n.got, line)
			n.gotMtx.Unlock()
			params := bytes.Split(line, []byte(" "))
			switch {
			case bytes.Equal(params[0], []byte("IDENTIFY")):
				l := make([]byte, 4)
				_, err := io.ReadFull(rdr, l)
				if err != nil {
					n.t.Log(err)
					goto exit
				}
				size := int32(binary.BigEndian.Uint32(l))
				b := make([]byte, size)
				_, err = io.ReadFull(rdr, b)
				if err != nil {
					n.t.Log(err)
					goto exit
				}
				n.t.Logf("%s", b)
			case bytes.Equal(params[0], []byte("RDY")):
				rdy, _ := strconv.Atoi(string(params[1]))
				rdyCount = rdy
			case bytes.Equal(params[0], []byte("FIN")):
			case bytes.Equal(params[0], []byte("REQ")):
			case bytes.Equal(params[0], []byte("CLS")):
				// ack the graceful close
				_, err := conn.Write(framedResponse(FrameTypeResponse, []byte("CLOSE_WAIT")))
				if err != nil {
					n.t.Log(err)
				}
				readDoneChan <- 1
				if n.holdAfterCloseWait {
					// nsqd keeps the connection while in-flight work drains.
					// Block on the raw connection (never the shared buffered
					// reader, which the line-reader goroutine still owns)
					// until the client tears the socket down.
					io.Copy(io.Discard, conn)
				}
				goto exit
			}
			readDoneChan <- 1
		case <-scriptTime:
			inst := n.script[idx]
			if bytes.Equal(inst.body, []byte("exit")) {
				goto exit
			}
			if inst.frameType == FrameTypeMessage {
				if rdyCount == 0 {
					n.t.Log("!!! RDY == 0")
					scriptTime = time.After(n.script[idx+1].delay)
					continue
				}
				rdyCount--
			}
			_, err := conn.Write(framedResponse(inst.frameType, inst.body))
			if err != nil {
				n.t.Log(err)
				goto exit
			}
			scriptTime = time.After(n.script[idx+1].delay)
			idx++
		}
	}

exit:
	// shut the accept loop so listen() closes exitChan, then release this
	// client; the test cleanup waits on the WaitGroup before teardown
	n.closeListener()
	conn.Close()
}

func framedResponse(frameType int32, data []byte) []byte {
	var w bytes.Buffer

	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	_, err := w.Write(beBuf)
	if err != nil {
		return nil
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	_, err = w.Write(beBuf)
	if err != nil {
		return nil
	}

	_, _ = w.Write(data)
	return w.Bytes()
}

type testHandler struct{}

// stopConsumerOnCleanup makes sure the consumer and all of its connection
// goroutines wind down before the test (and its mock nsqd) are torn down, so
// nothing logs from a detached goroutine after completion.
func stopConsumerOnCleanup(t *testing.T, q *Consumer) {
	t.Helper()
	t.Cleanup(func() {
		q.Stop()
		select {
		case <-q.StopChan:
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop cleanly")
		}
	})
}

func (h *testHandler) HandleMessage(message *Message) error {
	switch string(message.Body) {
	case "requeue":
		message.Requeue(-1)
		return nil
	case "requeue_no_backoff_1":
		if message.Attempts > 1 {
			return nil
		}
		message.RequeueWithoutBackoff(-1)
		return nil
	case "bad":
		return errors.New("bad")
	}
	return nil
}

func frameMessage(m *Message) []byte {
	var b bytes.Buffer
	m.WriteTo(&b)
	return b.Bytes()
}

func TestConsumerBackoff(t *testing.T) {
	msgIDGood := MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	msgGood := NewMessage(msgIDGood, []byte("good"))

	msgIDBad := MessageID{'z', 'x', 'c', 'v', 'b', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	msgBad := NewMessage(msgIDBad, []byte("bad"))

	script := []instruction{
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgBad)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgBad)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		// needed to exit test
		instruction{200 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	n := newMockNSQD(t, script, addr.String())

	topicName := "test_consumer_commands" + strconv.Itoa(int(time.Now().Unix()))
	config := NewConfig()
	config.MaxInFlight = 5
	config.BackoffMultiplier = 10 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)
	q.AddHandler(&testHandler{})
	err := q.ConnectToNSQD(n.tcpAddr.String())
	if err != nil {
		t.Fatalf(err.Error())
	}
	stopConsumerOnCleanup(t, q)

	<-n.exitChan

	for i, r := range n.got {
		t.Logf("%d: %s", i, r)
	}

	expected := []string{
		"IDENTIFY",
		"SUB " + topicName + " ch",
		"RDY 5",
		fmt.Sprintf("FIN %s", msgIDGood),
		fmt.Sprintf("FIN %s", msgIDGood),
		fmt.Sprintf("FIN %s", msgIDGood),
		"RDY 0",
		fmt.Sprintf("REQ %s 0", msgIDBad),
		"RDY 1",
		"RDY 0",
		fmt.Sprintf("REQ %s 0", msgIDBad),
		"RDY 1",
		"RDY 0",
		fmt.Sprintf("FIN %s", msgIDGood),
		"RDY 1",
		"RDY 5",
		fmt.Sprintf("FIN %s", msgIDGood),
	}
	if len(n.got) != len(expected) {
		t.Fatalf("we got %d commands != %d expected", len(n.got), len(expected))
	}
	for i, r := range n.got {
		if string(r) != expected[i] {
			t.Fatalf("cmd %d bad %s != %s", i, r, expected[i])
		}
	}
}

func TestConsumerRequeueNoBackoff(t *testing.T) {
	msgIDGood := MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	msgIDRequeue := MessageID{'r', 'e', 'q', 'v', 'b', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	msgIDRequeueNoBackoff := MessageID{'r', 'e', 'q', 'n', 'b', 'a', 'c', 'k', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}

	msgGood := NewMessage(msgIDGood, []byte("good"))
	msgRequeue := NewMessage(msgIDRequeue, []byte("requeue"))
	msgRequeueNoBackoff := NewMessage(msgIDRequeueNoBackoff, []byte("requeue_no_backoff_1"))

	script := []instruction{
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgRequeue)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgRequeueNoBackoff)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		// needed to exit test
		instruction{100 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	n := newMockNSQD(t, script, addr.String())

	topicName := "test_requeue" + strconv.Itoa(int(time.Now().Unix()))
	config := NewConfig()
	config.MaxInFlight = 1
	config.BackoffMultiplier = 10 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)
	q.AddHandler(&testHandler{})
	err := q.ConnectToNSQD(n.tcpAddr.String())
	if err != nil {
		t.Fatalf(err.Error())
	}
	stopConsumerOnCleanup(t, q)

	select {
	case <-n.exitChan:
		t.Log("clean exit")
	case <-time.After(500 * time.Millisecond):
		t.Log("timeout")
	}

	for i, r := range n.got {
		t.Logf("%d: %s", i, r)
	}

	expected := []string{
		"IDENTIFY",
		"SUB " + topicName + " ch",
		"RDY 1",
		"RDY 0",
		fmt.Sprintf("REQ %s 0", msgIDRequeue),
		"RDY 1",
		"RDY 0",
		fmt.Sprintf("REQ %s 0", msgIDRequeueNoBackoff),
		"RDY 1",
		fmt.Sprintf("FIN %s", msgIDGood),
	}
	if len(n.got) != len(expected) {
		t.Fatalf("we got %d commands != %d expected", len(n.got), len(expected))
	}
	for i, r := range n.got {
		if string(r) != expected[i] {
			t.Fatalf("cmd %d bad %s != %s", i, r, expected[i])
		}
	}
}

func TestConsumerBackoffDisconnect(t *testing.T) {
	msgIDGood := MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	msgIDRequeue := MessageID{'r', 'e', 'q', 'v', 'b', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}

	msgGood := NewMessage(msgIDGood, []byte("good"))
	msgRequeue := NewMessage(msgIDRequeue, []byte("requeue"))

	script := []instruction{
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgRequeue)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgRequeue)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		// needed to exit test
		instruction{100 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	n := newMockNSQD(t, script, addr.String())

	topicName := "test_requeue" + strconv.Itoa(int(time.Now().Unix()))
	config := NewConfig()
	config.MaxInFlight = 5
	config.BackoffMultiplier = 10 * time.Millisecond
	config.LookupdPollInterval = 10 * time.Millisecond
	config.RDYRedistributeInterval = 10 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)
	q.AddHandler(&testHandler{})
	err := q.ConnectToNSQD(n.tcpAddr.String())
	if err != nil {
		t.Fatalf(err.Error())
	}
	stopConsumerOnCleanup(t, q)

	select {
	case <-n.exitChan:
		t.Log("clean exit")
	case <-time.After(500 * time.Millisecond):
		t.Log("timeout")
	}

	for i, r := range n.got {
		t.Logf("%d: %s", i, r)
	}

	expected := []string{
		"IDENTIFY",
		"SUB " + topicName + " ch",
		"RDY 5",
		fmt.Sprintf("FIN %s", msgIDGood),
		"RDY 0",
		fmt.Sprintf("REQ %s 0", msgIDRequeue),
		"RDY 1",
		"RDY 0",
		fmt.Sprintf("REQ %s 0", msgIDRequeue),
		"RDY 1",
		"RDY 0",
		fmt.Sprintf("FIN %s", msgIDGood),
		"RDY 1",
	}
	if len(n.got) != len(expected) {
		t.Fatalf("we got %d commands != %d expected", len(n.got), len(expected))
	}
	for i, r := range n.got {
		if string(r) != expected[i] {
			t.Fatalf("cmd %d bad %s != %s", i, r, expected[i])
		}
	}

	script = []instruction{
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		// needed to exit test
		instruction{100 * time.Millisecond, -1, []byte("exit")},
	}

	n = newMockNSQD(t, script, n.tcpAddr.String())

	select {
	case <-n.exitChan:
		t.Log("clean exit")
	case <-time.After(500 * time.Millisecond):
		t.Log("timeout")
	}

	for i, r := range n.got {
		t.Logf("%d: %s", i, r)
	}

	expected = []string{
		"IDENTIFY",
		"SUB " + topicName + " ch",
		"RDY 1",
		"RDY 5",
		fmt.Sprintf("FIN %s", msgIDGood),
		fmt.Sprintf("FIN %s", msgIDGood),
	}
	if len(n.got) != len(expected) {
		t.Fatalf("we got %d commands != %d expected", len(n.got), len(expected))
	}
	for i, r := range n.got {
		if string(r) != expected[i] {
			t.Fatalf("cmd %d bad %s != %s", i, r, expected[i])
		}
	}
}

func TestConsumerPause(t *testing.T) {
	msgIDGood := MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}

	msgGood := NewMessage(msgIDGood, []byte("good"))

	script := []instruction{
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		// needed to exit test
		instruction{200 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	n := newMockNSQD(t, script, addr.String())

	topicName := "test_pause" + strconv.Itoa(int(time.Now().Unix()))
	config := NewConfig()
	config.MaxInFlight = 5
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)
	q.AddHandler(&testHandler{})
	err := q.ConnectToNSQD(n.tcpAddr.String())
	if err != nil {
		t.Fatalf(err.Error())
	}
	stopConsumerOnCleanup(t, q)

	timeoutCh := time.After(500 * time.Millisecond)
	pauseCh := time.After(50 * time.Millisecond)
	unpauseCh := time.After(75 * time.Millisecond)
	for {
		select {
		case <-n.exitChan:
			t.Log("clean exit")
			goto done
		case <-timeoutCh:
			t.Log("timeout")
			goto done
		case <-pauseCh:
			q.ChangeMaxInFlight(0)
		case <-unpauseCh:
			q.ChangeMaxInFlight(config.MaxInFlight)
		}
	}
done:

	for i, r := range n.got {
		t.Logf("%d: %s", i, r)
	}

	expected := []string{
		"IDENTIFY",
		"SUB " + topicName + " ch",
		"RDY 5",
		fmt.Sprintf("FIN %s", msgIDGood),
		"RDY 0",
		"RDY 5",
	}
	if len(n.got) != len(expected) {
		t.Fatalf("we got %d commands != %d expected", len(n.got), len(expected))
	}
	for i, r := range n.got {
		if string(r) != expected[i] {
			t.Fatalf("cmd %d bad %s != %s", i, r, expected[i])
		}
	}
}
