package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/nats-io/nats.go"
)

func usage() { _ = "STUB: not implemented"; return }

func showUsageAndExit(exitcode int) { _ = "STUB: not implemented"; return }

func printMsg(m *nats.Msg, i int) { _ = "STUB: not implemented"; return }

func printStatusMsg(m *nats.Msg, i int) { _ = "STUB: not implemented"; return }

type serviceStatus struct {
	Id  string `json:"id"`
	Geo string `json:"geo"`
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var nkeyFile = flag.String("nkey", "", "NKey Seed File")
	var serviceId = flag.String("id", "NATS Echo Service", "Identifier for this service")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var showHelp = flag.Bool("h", false, "Show help message")
	var geoloc = flag.Bool("geo", false, "Display geo location of echo service")
	var geo string = "unknown"

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

	if *geoloc {
		geo = lookupGeo()
	}

	opts := []nats.Option{nats.Name(*serviceId)}
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

	subj, iEcho, iStatus := args[0], 0, 0
	statusSubj := subj + ".status"

	nc.QueueSubscribe(subj, "echo", func(msg *nats.Msg) {
		iEcho++
		printMsg(msg, iEcho)
		if msg.Reply != "" {

			var payload []byte
			if geo != "unknown" {
				payload = []byte(fmt.Sprintf("[%s]: %q", geo, msg.Data))
			} else {
				payload = msg.Data
			}
			nc.Publish(msg.Reply, payload)
		}
	})
	nc.Subscribe(statusSubj, func(msg *nats.Msg) {
		iStatus++
		printStatusMsg(msg, iStatus)
		if msg.Reply != "" {
			payload, _ := json.Marshal(&serviceStatus{Id: *serviceId, Geo: geo})
			nc.Publish(msg.Reply, payload)
		}
	})
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Echo Service ID: [%s]", *serviceId)
	log.Printf("Echo Service listening on [%s]\n", subj)
	log.Printf("Echo Service (Status) listening on [%s]\n", statusSubj)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)

	go func() {

		<-c
		log.Printf("<caught signal - draining>")
		nc.Drain()
	}()

	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}

func setupConnOptions(opts []nats.Option) []nats.Option { _ = "STUB: not implemented"; return nil }

type geo struct {
	Region  string
	Country string
}

func lookupGeo() string { _ = "STUB: not implemented"; return "" }
