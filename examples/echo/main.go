package main

import (
	"context"
	"encoding/json"
	"fmt"
	pluhub "github.com/MoeClub-Open/PluHub-SDK-Go"
	"os"
	"time"
)

func main() {
	var host *pluhub.Host
	p := pluhub.Plugin{Init: func(h *pluhub.Host) error { host = h; return nil }, Handlers: map[string]pluhub.Handler{"example.echo@1": func(ctx context.Context, input json.RawMessage) (any, error) {
		var v map[string]any
		_ = json.Unmarshal(input, &v)
		if v["getHostConfig"] == true {
			var config map[string]any
			err := host.Call(ctx, "example.config@1", nil, &config)
			return config, err
		}
		if v["getConfig"] == true {
			var config map[string]any
			err := json.Unmarshal(pluhub.RequestConfig(ctx), &config)
			return config, err
		}
		if v["wait"] == true {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Minute):
			}
		}
		return v, nil
	}}}
	if e := p.Run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
