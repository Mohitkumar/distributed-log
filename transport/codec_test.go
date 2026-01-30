package transport

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeFrame(t *testing.T) {
	payload := []byte("hello world")
	var buf bytes.Buffer
	if err := EncodeFrame(&buf, payload); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("got %q, want %q", got, payload)
	}
}

func TestDecodeFrame_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := EncodeFrame(&buf, nil); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("got len %d, want 0", len(got))
	}
}
