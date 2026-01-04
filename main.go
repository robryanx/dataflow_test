package main

import (
	"fmt"
	"log"
	"os"

	"dataflow_test/internal/tools"
)

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "bootstrap":
		err = tools.RunBootstrap(args)
	case "writer":
		err = tools.RunWriter(args)
	case "pubsub-setup":
		err = tools.RunPubsubSetup(args)
	case "outbox-subscribe":
		err = tools.RunOutboxSubscribe(args)
	case "outbox-insert":
		err = tools.RunOutboxInsert(args)
	case "bq-sink":
		err = tools.RunBigQuerySink(args)
	case "bq-check":
		err = tools.RunBigQueryCheck(args)
	case "spanner-check":
		err = tools.RunSpannerCheck(args)
	default:
		usage()
		err = fmt.Errorf("unknown command: %s", cmd)
	}

	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func usage() {
	fmt.Println("usage: go run . <command> [args]")
	fmt.Println("commands: bootstrap, writer, pubsub-setup, outbox-subscribe, outbox-insert, bq-sink, bq-check, spanner-check")
}
