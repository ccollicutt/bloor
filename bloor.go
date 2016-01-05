package main

// NOTE(curtis): This is just a direct copy of
// https://github.com/phunt/zk-smoketest/blob/master/zk-smoketest.py
// but written in go. Lots more to do, just wanted to try a direct copy.

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	"github.com/samuel/go-zookeeper/zk"
)

type bloorConfig struct {
	rootZnodeName string
	verbose       bool
}

func run(conf bloorConfig) {

	// First get the servers from the environment variable
	zksStr := os.Getenv("ZOOKEEPER_SERVERS")
	var servers []string
	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	// Children are ephemeral
	childFlags := int32(zk.FlagEphemeral)
	rootZnode := fmt.Sprintf("/%s", conf.rootZnodeName)

	// Split the server string into an array
	if len(zksStr) == 0 {
		log.Fatal("ZOOKEEPER_SERVERS environment variable is empty")
	} else {
		log.Printf("ZOOKEEPER_SERVERS: %s", zksStr)
		servers = strings.Split(zksStr, ",")
		for i := 0; i < len(servers); i++ {
			log.Printf("Server %d: %s", i, servers[i])
		}
	}

	// Setup sessions/connections
	conns := make([]*zk.Conn, len(servers))
	for i := range servers {
		// zk.Connect expects an array
		s := make([]string, 1)
		s[0] = servers[i]
		conn, _, err := zk.Connect(s, time.Second)
		if err != nil {
			log.Fatal(err)
		}
		conns[i] = conn
	}

	// Check if rootpath exists already on the first server.
	// If not, create it.
	exists, _, err := conns[0].Exists(rootZnode)
	if err != nil {
		log.Fatal(err)
	}
	if exists == true {
		log.Printf("Root znode %s already exists, not creating", rootZnode)
	} else {
		rootZnodeContent := "bloor root znode"
		_, err := conns[0].Create(rootZnode, []byte(rootZnodeContent),
			flags, acl)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Created znode root %s", conf.rootZnodeName)
		}
	}

	// Get children
	children, _, err := conns[0].Children(rootZnode)
	if err != nil {
		log.Fatal(err)
	}
	if len(children) > 0 {
		log.Fatalf("Children exist beneath root znode %s", rootZnode)
	} else {
		log.Printf("Root znode %s has no children", rootZnode)
	}

	// Create child nodes
	for i, conn := range conns {
		childZnode := fmt.Sprintf("%s/session_%d", rootZnode, i)
		childZnodeContent := fmt.Sprintf("child-%d", i)

		// First sync up
		_, err := conn.Sync(rootZnode)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Synced connection %d", i)
		}

		// Now create nodes
		_, err = conn.Create(childZnode, []byte(childZnodeContent),
			childFlags, acl)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Child znode %s was created", childZnode)
		}
	}

	// Sync all the servers, check children, setup watchers
	watchers := make([]<-chan zk.Event, len(conns))
	for i, conn := range conns {

		// Sync up again
		_, err := conn.Sync(rootZnode)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Synced connection %d", i)
		}

		// Check if expected number of children
		children, _, err := conn.Children(rootZnode)
		if err != nil {
			log.Fatal(err)
		}
		if len(children) != len(conns) {
			log.Fatalf("Expected children: %d, found children: %d", len(children),
				len(conns))
		} else {
			log.Printf("Found correct number of children (%d) in connection %d",
				len(conns), i)
		}

		// Set watchers
		snapshots, _, events, err := conn.ChildrenW(rootZnode)
		if err != nil {
			log.Fatalf("Error setting up watch %s", err)
		} else {
			log.Printf("Set watcher on rootpath %s", rootZnode)
			// NOTE(curtis): Does this make sense?
			for j, v := range snapshots {
				log.Printf("Watching child %s/%s on session %d", rootZnode, v, j)
			}
			watchers[i] = events
		}
	}

	// Delete the child znodes
	for i, conn := range conns {
		childZnode := fmt.Sprintf("%s/session_%d", rootZnode, i)
		conn.Delete(childZnode, -1)
	}

	// Check the watchers
	for i, event := range watchers {
		// Sync up
		_, err := conns[i].Sync(rootZnode)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Synced connection %d", i)
		}

		log.Printf("Length of event channel for session %d is %d", i, len(event))

		// Check how many waches fired
		if len(event) != 1 {
			log.Fatalf("Watcher for session %d missed event", i)
		}
		msg := <-event
		log.Printf("Event type %s occured on path %s with session %d",
			zk.EventType(msg.Type), msg.Path, i)
	}

	// Sync first session to delete
	_, err = conns[0].Sync(rootZnode)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Synced first connection to delete rootpath")
	}
	// Delete rootpath/rootzode
	err = conns[0].Delete(rootZnode, -1)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Deleted rootpath %s", rootZnode)
	}

	// Finally close all connections
	// FIXME: Could be defer?
	for _, conn := range conns {
		conn.Close()
	}
}

func main() {

	app := cli.NewApp()
	app.Name = "bloor"
	app.Version = "0.0.1"
	app.Usage = "Zookeeper performance testing tool"

	var rootZnodeName string
	var verbose bool

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "znode-root, r",
			Value:       "bloor-smoketest",
			Usage:       "Root name of znode",
			Destination: &rootZnodeName,
		},
		cli.BoolFlag{
			Name:        "verbose, V",
			Usage:       "Print logs",
			Destination: &verbose,
		},
	}

	app.Action = func(c *cli.Context) {

		var conf bloorConfig
		conf.verbose = verbose
		conf.rootZnodeName = rootZnodeName

		// Log to stdout to startup
		log.SetOutput(os.Stdout)
		log.Printf("Bloor smoketest started (use -V for more logging)")
		// Now only log if verbose is set
		if verbose == false {
			log.SetOutput(ioutil.Discard)
		}

		// Do the work
		run(conf)

		// Back to normal logging
		log.SetOutput(os.Stdout)
		log.Printf("Bloor run completed")
	}

	app.Run(os.Args)
}
