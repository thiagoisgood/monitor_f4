package fleet

import (
	"bytes"
	"compress/gzip"
	"testing"
)

func TestInventoryTransportRoundtripAndBound(t *testing.T) {
	for _, test := range []struct {
		name string
		size int
		ok   bool
	}{{"normal", 100000, true}, {"exact_limit", 512 * 1024, true}, {"oversized", 512*1024 + 1, false}} {
		t.Run(test.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte("x"), test.size)
			var wire bytes.Buffer
			writer := gzip.NewWriter(&wire)
			writer.Write(payload)
			writer.Close()
			decoded, err := unpackInventory(wire.Bytes())
			if (err == nil) != test.ok {
				t.Fatalf("unexpected transport result: %v", err)
			}
			if test.ok && !bytes.Equal(decoded, payload) {
				t.Fatal("transport changed inventory contents")
			}
		})
	}
	if _, err := unpackInventory([]byte("invalid gzip")); err == nil {
		t.Fatal("invalid transport accepted")
	}
}
