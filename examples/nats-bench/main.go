package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"
)

const (
	DefaultNumMsgs     = 100000
	DefaultNumPubs     = 1
	DefaultNumSubs     = 0
	DefaultMessageSize = 128
)

func usage() { _ = "STUB: not implemented"; return }

func showUsageAndExit(exitcode int) { _ = "STUB: not implemented"; return }

var benchmark *bench.Benchmark

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var tls = flag.Bool("tls", false, "Use TLS Secure Connection")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of Concurrent Publishers")
	var numSubs = flag.Int("ns", DefaultNumSubs, "Number of Concurrent Subscribers")
	var numMsgs = flag.Int("n", DefaultNumMsgs, "Number of Messages to Publish")
	var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message.")
	var csvFile = flag.String("csv", "", "Save bench data to csv file")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var nkeyFile = flag.String("nkey", "", "NKey Seed File")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 1 {
		showUsageAndExit(1)
	}

	if *numMsgs <= 0 {
		log.Fatal("Number of messages should be greater than zero.")
	}

	opts := []nats.Option{nats.Name("NATS Benchmark")}

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

	if *tls {
		opts = append(opts, nats.Secure(nil))
	}

	benchmark = bench.NewBenchmark("NATS", *numSubs, *numPubs)

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(*numPubs + *numSubs)

	startwg.Add(*numSubs)
	for i := 0; i < *numSubs; i++ {
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			log.Fatalf("Can't connect: %v\n", err)
		}
		defer nc.Close()

		go runSubscriber(nc, &startwg, &donewg, *numMsgs, *msgSize)
	}
	startwg.Wait()

	startwg.Add(*numPubs)
	pubCounts := bench.MsgsPerClient(*numMsgs, *numPubs)
	for i := 0; i < *numPubs; i++ {
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			log.Fatalf("Can't connect: %v\n", err)
		}
		defer nc.Close()

		go runPublisher(nc, &startwg, &donewg, pubCounts[i], *msgSize)
	}

	log.Printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d]\n", *numMsgs, *msgSize, *numPubs, *numSubs)

	startwg.Wait()
	donewg.Wait()

	benchmark.Close()

	fmt.Print(benchmark.Report())

	if len(*csvFile) > 0 {
		csv := benchmark.CSV()
		os.WriteFile(*csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", *csvFile)
	}
}

func runPublisher(nc *nats.Conn, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int) {
	_ = "STUB: not implemented"
	return
}

func runSubscriber(nc *nats.Conn, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int) {
	_ = "STUB: not implemented"
	return
}
