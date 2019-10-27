package main

import (
	"flag"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/prometheus/common/log"
)

var logger = hclog.New(&hclog.LoggerOptions{
	Level:      hclog.Warn,
	Name:       "redbull",
	JSONFormat: true,
})

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	plugin, err := newRedBull()
	if err != nil {
		log.Warn("error creating redbull", "err", err)
	}

	defer plugin.Close()

	grpc.Serve(plugin)
}
