package main

import (
	"flag"
	"log"

	"github.com/nats-io/nats.go"
)

func usage() { _ = "STUB: not implemented"; return }

func showUsageAndExit(exitcode int) { _ = "STUB: not implemented"; return }

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var nkeyFile = flag.String("nkey", "", "NKey Seed File")
	var tlsClientCert = flag.String("tlscert", "", "TLS client certificate file")
	var tlsClientKey = flag.String("tlskey", "", "Private key file for client certificate")
	var tlsCACert = flag.String("tlscacert", "", "CA certificate to verify peer against")
	var reply = flag.String("reply", "", "Sets a specific reply subject")
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

	opts := []nats.Option{nats.Name("NATS Sample Publisher")}

	if *userCreds != "" && *nkeyFile != "" {
		log.Fatal("specify -seed or -creds")
	}

	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	if *tlsClientCert != "" && *tlsClientKey != "" {
		opts = append(opts, nats.ClientCert(*tlsClientCert, *tlsClientKey))
	}

	if *tlsCACert != "" {
		opts = append(opts, nats.RootCAs(*tlsCACert))
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
	defer nc.Close()

	subj, msg := args[0], []byte(args[1])

	if reply != nil && *reply != "" {
		nc.PublishRequest(subj, *reply, msg)
	} else {
		nc.Publish(subj, msg)
	}

	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Published [%s] : '%s'\n", subj, msg)
	}
}
