package pluhub

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func TestSharedFixtures(t *testing.T) {
	for _, v := range []struct {
		name  string
		valid bool
	}{{"valid", true}, {"invalid", false}, {"dependencies.valid", true}} {
		b, e := os.ReadFile("../PluHub-Spec/fixtures/" + v.name + ".json")
		if e != nil {
			t.Fatal(e)
		}
		var m Manifest
		if e = json.Unmarshal(b, &m); e != nil {
			t.Fatal(e)
		}
		if (ValidateManifest(m) == nil) != v.valid {
			t.Fatal(v.name)
		}
		if v.valid {
			m.Category = "community.monitoring"
			if ValidateManifest(m) != nil {
				t.Fatal("custom category was rejected")
			}
			m.Category = "Invalid Category"
			if ValidateManifest(m) == nil {
				t.Fatal("invalid category was accepted")
			}
		}
	}
}
func TestManifestDependencyMetadata(t *testing.T) {
	b, err := os.ReadFile("../PluHub-Spec/fixtures/dependencies.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m.Dependencies["example.provider"] != "^1.2.0" || len(m.OptionalRequires) != 1 {
		t.Fatal("dependency metadata lost")
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var restored Manifest
	if err = json.Unmarshal(encoded, &restored); err != nil || restored.Dependencies["example.provider"] != "^1.2.0" {
		t.Fatal("dependency roundtrip failed")
	}
	m.Dependencies = map[string]string{m.ID: "*"}
	if ValidateManifest(m) == nil {
		t.Fatal("self dependency accepted")
	}
}

func TestInitializeCanCallHost(t *testing.T) {
	input, send := io.Pipe()
	receive, output := io.Pipe()
	defer input.Close()
	defer send.Close()
	defer receive.Close()
	defer output.Close()
	p := Plugin{Handlers: map[string]Handler{}, Init: func(h *Host) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var result string
		if e := h.Call(ctx, "example.host@1", map[string]string{"hello": "host"}, &result); e != nil {
			return e
		}
		if result != "ready" {
			return errors.New("wrong host result")
		}
		return nil
	}}
	done := make(chan error, 1)
	go func() { done <- p.Serve(context.Background(), input, output) }()
	encoder := json.NewEncoder(send)
	scanner := bufio.NewScanner(receive)
	_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "plugin.initialize", "params": map[string]any{"protocolVersion": 1}})
	if !scanner.Scan() {
		t.Fatal("missing Host call")
	}
	var request envelope
	if e := json.Unmarshal(scanner.Bytes(), &request); e != nil {
		t.Fatal(e)
	}
	if request.Method != "host.call" {
		t.Fatal(request.Method)
	}
	_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": "ready"})
	if !scanner.Scan() {
		t.Fatal("missing initialize result")
	}
	var initialized envelope
	_ = json.Unmarshal(scanner.Bytes(), &initialized)
	if initialized.Error != nil {
		t.Fatal(initialized.Error)
	}
	_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "plugin.stop"})
	if !scanner.Scan() {
		t.Fatal("missing stop result")
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("plugin did not stop")
	}
}
