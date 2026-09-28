package group

// tlsRecords follows TLS record boundaries in the bytes a server sends. A censor that lets
// a connection start and then stops passing packets (Russian TSPU does this to parts of
// Cloudflare and other CDNs after ~16-20 KB) leaves the stream stuck inside a record; a
// server never does, it always finishes the record it started. So "no data for a while,
// in the middle of a record" is a strong sign of a cut, which the silence heuristics miss
// because the answer did begin.
type tlsRecords struct {
	valid  bool
	header [5]byte
	have   int // header bytes collected
	need   int // body bytes still expected in the current record
}

const maxTLSRecord = 16384 + 2048

func newTLSRecords() *tlsRecords {
	return &tlsRecords{valid: true}
}

func (t *tlsRecords) feed(p []byte) {
	for t.valid && len(p) > 0 {
		if t.need > 0 {
			n := min(t.need, len(p))
			t.need -= n
			p = p[n:]
			continue
		}
		n := copy(t.header[t.have:], p)
		t.have += n
		p = p[n:]
		if t.have < len(t.header) {
			return
		}
		t.have = 0
		kind, major := t.header[0], t.header[1]
		length := int(t.header[3])<<8 | int(t.header[4])
		// change_cipher_spec, alert, handshake, application_data in a 3.x record layer;
		// anything else means this is not TLS and the tracker gives up
		if kind < 20 || kind > 23 || major != 3 || length == 0 || length > maxTLSRecord {
			t.valid = false
			return
		}
		t.need = length
	}
}

// inside reports a stream stopped part way through a record header or body.
func (t *tlsRecords) inside() bool {
	return t.valid && (t.need > 0 || t.have > 0)
}
