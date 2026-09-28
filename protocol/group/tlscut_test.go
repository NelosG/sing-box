package group

import "testing"

func record(kind byte, length int) []byte {
	b := []byte{kind, 3, 3, byte(length >> 8), byte(length)}
	return append(b, make([]byte, length)...)
}

func TestTLSRecordsAtBoundary(t *testing.T) {
	r := newTLSRecords()
	stream := append(record(22, 90), record(23, 16384)...)
	stream = append(stream, record(23, 300)...)
	// fed in awkward pieces, as TCP delivers it
	for len(stream) > 0 {
		n := min(len(stream), 1337)
		r.feed(stream[:n])
		stream = stream[n:]
	}
	if r.inside() {
		t.Fatal("complete records must not look cut")
	}
}

func TestTLSRecordsCutInsideBody(t *testing.T) {
	r := newTLSRecords()
	stream := append(record(22, 90), record(23, 16384)...)
	r.feed(stream[:5+90+5+4000])
	if !r.inside() {
		t.Fatal("a stream stopped 4000 bytes into a 16 KB record is cut")
	}
}

func TestTLSRecordsCutInsideHeader(t *testing.T) {
	r := newTLSRecords()
	stream := append(record(23, 10), 23, 3)
	r.feed(stream)
	if !r.inside() {
		t.Fatal("a stream stopped inside a record header is cut")
	}
}

func TestTLSRecordsIgnorePlainHTTP(t *testing.T) {
	r := newTLSRecords()
	r.feed([]byte("HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n"))
	if r.inside() {
		t.Fatal("plain HTTP is not tracked")
	}
}
