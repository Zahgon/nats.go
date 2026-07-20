package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
)

func usage() { _ = "STUB: not implemented"; return }

func showUsageAndExit(exitcode int) { _ = "STUB: not implemented"; return }

func printMsg(m *nats.Msg, i int) { _ = "STUB: not implemented"; return }

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var nkeyFile = flag.String("nkey", "", "NKey Seed File")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 2 {
		showUsageAndExit(1)
	}

	opts := []nats.Option{nats.Name("NATS Sample Queue Subscriber")}
	opts = setupConnOptions(opts)

	if *userCreds != "" && *nkeyFile != "" {
		log.Fatal("specify -seed or -creds")
	}

	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	if *nkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(*nkeyFile)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, opt)
	}

	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}

	subj, queue, i := args[0], args[1], 0

	nc.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		i++
		printMsg(msg, i)
	})
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s], queue group [%s]", subj, queue)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Println()
	log.Printf("Draining...")
	nc.Drain()
	log.Fatalf("Exiting")
}

func setupConnOptions(opts []nats.Option) []nats.Option { _ = "STUB: not implemented"; return nil }
