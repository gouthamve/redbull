package main

import (
	"flag"

	"log"

	"github.com/gouthamve/redbull/pkg/redbull"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
)

//var logger = hclog.New(&hclog.LoggerOptions{
//Level:      hclog.Warn,
//Name:       "redbull",
//JSONFormat: true,
//})

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	plugin, err := redbull.NewRedBull(redbull.Config{})
	if err != nil {
		log.Panicln("error creating redbull", "err", err)
	}

	defer plugin.Close()

	grpc.Serve(plugin)
}
