// Package pluhub implements the language-neutral stdio control protocol v1.
package pluhub

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
)

type Handler func(context.Context, json.RawMessage) (any, error)
type Manifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	ID            string `json:"id"`
	Name          string `json:"name,omitempty"`
	Description   string `json:"description,omitempty"`
	Category      string `json:"category,omitempty"`
	CodeReload    string `json:"codeReload,omitempty"`
	Version       string `json:"version"`
	Host          string `json:"host"`
	Runtime       struct {
		Kind  string `json:"kind"`
		Entry string `json:"entry"`
	} `json:"runtime"`
	Provides         []string          `json:"provides"`
	Requires         []string          `json:"requires"`
	OptionalRequires []string          `json:"optionalRequires,omitempty"`
	Dependencies     map[string]string `json:"dependencies,omitempty"`
	Permissions      []string          `json:"permissions"`
}

func ValidateManifest(m Manifest) error {
	if len(m.Dependencies) > 128 {
		return errors.New("too many plugin dependencies")
	}
	if m.Category != "" && (len(m.Category) > 64 || !regexp.MustCompile(`^[a-z][a-z0-9.-]*$`).MatchString(m.Category)) {
		return errors.New("invalid plugin category")
	}
	if m.CodeReload != "" && m.CodeReload != "restart" && m.CodeReload != "parallel" {
		return errors.New("invalid code reload policy")
	}
	if m.SchemaVersion != 1 || !regexp.MustCompile(`^[a-z][a-z0-9.-]{1,95}$`).MatchString(m.ID) || !regexp.MustCompile(`^\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?$`).MatchString(m.Version) || m.Host == "" || m.Runtime.Entry == "" || (m.Runtime.Kind != "native" && m.Runtime.Kind != "process") {
		return errors.New("invalid manifest")
	}
	for id, versionRange := range m.Dependencies {
		if id == m.ID || !regexp.MustCompile(`^[a-z][a-z0-9.-]{1,95}$`).MatchString(id) || strings.TrimSpace(versionRange) == "" || len(versionRange) > 256 {
			return errors.New("invalid plugin dependency")
		}
	}
	for _, list := range [][]string{m.Provides, m.Requires, m.OptionalRequires} {
		seen := map[string]bool{}
		for _, name := range list {
			if !regexp.MustCompile(`^[a-z][a-z0-9.-]*@[1-9][0-9]*$`).MatchString(name) || seen[name] {
				return errors.New("invalid capability")
			}
			seen[name] = true
		}
	}
	for _, required := range m.Requires {
		for _, optional := range m.OptionalRequires {
			if required == optional {
				return errors.New("capability is both required and optional")
			}
		}
	}
	return nil
}

type envelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int            `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   any             `json:"error,omitempty"`
}
type Host struct {
	done    chan struct{}
	Config  json.RawMessage
	DataDir string
	mu      sync.Mutex
	writer  io.Writer
	next    int
	pending map[int]chan envelope
}

func (h *Host) send(v envelope) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > 1024*1024 {
		return errors.New("message too large")
	}
	_, e = h.writer.Write(append(b, '\n'))
	return e
}
func (h *Host) Call(ctx context.Context, capability string, input any, result any) error {
	return h.request(ctx, "host.call", map[string]any{"capability": capability, "input": input, "contextId": invocationID(ctx)}, result)
}

type CapabilityProvider struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

func (h *Host) Providers(ctx context.Context, capability string) ([]CapabilityProvider, error) {
	var result []CapabilityProvider
	err := h.request(ctx, "host.providers", map[string]any{"capability": capability, "contextId": invocationID(ctx)}, &result)
	return result, err
}

func (h *Host) CallProvider(ctx context.Context, capability, providerID string, input any, result any) error {
	return h.request(ctx, "host.call", map[string]any{"capability": capability, "providerId": providerID, "input": input, "contextId": invocationID(ctx)}, result)
}

func (h *Host) Secret(ctx context.Context, reference string) (string, error) {
	var result string
	e := h.request(ctx, "host.secret", map[string]string{"reference": reference, "contextId": invocationID(ctx)}, &result)
	return result, e
}
func (h *Host) request(ctx context.Context, method string, params any, result any) error {
	h.mu.Lock()
	h.next++
	id := h.next
	ch := make(chan envelope, 1)
	h.pending[id] = ch
	h.mu.Unlock()
	defer func() { h.mu.Lock(); delete(h.pending, id); h.mu.Unlock() }()
	b, _ := json.Marshal(params)
	if e := h.send(envelope{JSONRPC: "2.0", ID: &id, Method: method, Params: b}); e != nil {
		return e
	}
	select {
	case <-ctx.Done():
		b, _ := json.Marshal(map[string]int{"id": id})
		_ = h.send(envelope{JSONRPC: "2.0", Method: "$/cancelRequest", Params: b})
		return ctx.Err()
	case <-h.done:
		return errors.New("host channel closed")
	case message := <-ch:
		if message.Error != nil {
			return fmt.Errorf("host call failed: %v", message.Error)
		}
		b, _ := json.Marshal(message.Result)
		return json.Unmarshal(b, result)
	}
}

type Plugin struct {
	Handlers map[string]Handler
	Init     func(*Host) error
	Stop     func() error
}

func (p Plugin) Serve(ctx context.Context, input io.Reader, output io.Writer) error {
	ctx, cancelRoot := context.WithCancel(ctx)
	defer cancelRoot()
	h := &Host{writer: output, pending: map[int]chan envelope{}, done: make(chan struct{})}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var stateMu, activeMu sync.Mutex
	active := map[int]context.CancelFunc{}
	var workers, initializers sync.WaitGroup
	initialized, initializing, draining := false, false, false
	reply := func(id int, result any, err error) {
		message := envelope{JSONRPC: "2.0", ID: &id, Result: result}
		if err != nil {
			message.Result = nil
			message.Error = map[string]any{"code": -32000, "message": err.Error()}
		}
		_ = h.send(message)
	}
	cleanup := func() {
		cancelRoot()
		close(h.done)
		activeMu.Lock()
		for _, cancel := range active {
			cancel()
		}
		activeMu.Unlock()
		workers.Wait()
		initializers.Wait()
	}
	defer cleanup()
	for scanner.Scan() {
		var m envelope
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			return err
		}
		if m.JSONRPC != "2.0" {
			return errors.New("invalid RPC version")
		}
		if m.Method == "" && m.ID != nil {
			h.mu.Lock()
			ch := h.pending[*m.ID]
			h.mu.Unlock()
			if ch != nil {
				select {
				case ch <- m:
				default:
				}
			}
			continue
		}
		if m.Method == "$/cancelRequest" {
			var v struct {
				ID int `json:"id"`
			}
			_ = json.Unmarshal(m.Params, &v)
			activeMu.Lock()
			if cancel := active[v.ID]; cancel != nil {
				cancel()
			}
			activeMu.Unlock()
			continue
		}
		if m.ID == nil {
			continue
		}
		id := *m.ID
		switch m.Method {
		case "plugin.initialize":
			stateMu.Lock()
			duplicate := initialized || initializing
			initializing = true
			stateMu.Unlock()
			if duplicate {
				reply(id, nil, errors.New("already initialized"))
				continue
			}
			var v struct {
				ProtocolVersion int             `json:"protocolVersion"`
				Config          json.RawMessage `json:"config"`
				DataDir         string          `json:"dataDir"`
			}
			if err := json.Unmarshal(m.Params, &v); err != nil || v.ProtocolVersion != 1 {
				reply(id, nil, errors.New("unsupported protocol"))
				continue
			}
			h.Config = v.Config
			h.DataDir = v.DataDir
			initializers.Add(1)
			go func(id int) {
				defer initializers.Done()
				var err error
				if p.Init != nil {
					err = p.Init(h)
				}
				stateMu.Lock()
				initialized = err == nil
				initializing = false
				stateMu.Unlock()
				caps := []string{}
				for name := range p.Handlers {
					caps = append(caps, name)
				}
				reply(id, map[string]any{"protocolVersion": 1, "capabilities": caps}, err)
			}(id)
		case "plugin.health":
			stateMu.Lock()
			ready := initialized && !draining
			stateMu.Unlock()
			reply(id, map[string]bool{"ready": ready}, nil)
		case "plugin.drain":
			stateMu.Lock()
			draining = true
			stateMu.Unlock()
			reply(id, true, nil)
		case "plugin.stop":
			activeMu.Lock()
			for _, cancel := range active {
				cancel()
			}
			activeMu.Unlock()
			workers.Wait()
			var err error
			if p.Stop != nil {
				err = p.Stop()
			}
			reply(id, true, err)
			return nil
		case "capability.invoke":
			stateMu.Lock()
			ready := initialized && !draining
			stateMu.Unlock()
			if !ready {
				reply(id, nil, errors.New("plugin not ready"))
				continue
			}
			var v struct {
				Capability string          `json:"capability"`
				Input      json.RawMessage `json:"input"`
				Config     json.RawMessage `json:"config"`
				ContextID  string          `json:"contextId"`
			}
			if err := json.Unmarshal(m.Params, &v); err != nil {
				reply(id, nil, err)
				continue
			}
			handler := p.Handlers[v.Capability]
			if handler == nil {
				reply(id, nil, errors.New("unknown capability"))
				continue
			}
			callCtx, cancel := context.WithCancel(ctx)
			callCtx = context.WithValue(callCtx, configContextKey{}, v.Config)
			callCtx = context.WithValue(callCtx, invocationContextKey{}, v.ContextID)
			activeMu.Lock()
			active[id] = cancel
			activeMu.Unlock()
			workers.Add(1)
			go func(id int, params json.RawMessage, fn Handler) {
				defer workers.Done()
				defer cancel()
				value, err := fn(callCtx, params)
				reply(id, value, err)
				activeMu.Lock()
				delete(active, id)
				activeMu.Unlock()
			}(id, v.Input, handler)
		default:
			reply(id, nil, errors.New("unknown method"))
		}
	}
	return scanner.Err()
}

func (p Plugin) Run() error { return p.Serve(context.Background(), os.Stdin, os.Stdout) }

type configContextKey struct{}

// RequestConfig returns the immutable configuration snapshot for this invocation.
func RequestConfig(ctx context.Context) json.RawMessage {
	value, _ := ctx.Value(configContextKey{}).(json.RawMessage)
	return append(json.RawMessage(nil), value...)
}

type invocationContextKey struct{}

func invocationID(ctx context.Context) string {
	value, _ := ctx.Value(invocationContextKey{}).(string)
	return value
}
