package main

// NOTE(curtis): Originally this was a golang copy of
// https://github.com/phunt/zk-smoketest/blob/master/zk-smoketest.py
// Hopefully it's now improved and more go-idiomatic than the initial
// checkin. As usual lots more to do though. :)

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
	zkServers 		[]string
	rootZnode 		string
	verbose       bool
	acl						[]zk.ACL
	rootFlags			int32
	childFlags		int32
	conns 				[]*zk.Conn
}

func getServerArray (serverList string) []string {
	servers := strings.Split(serverList, ",")
	for i := 0; i < len(servers); i++ {
		log.Printf("Server %d: %s", i, servers[i])
	}
	return servers
}

// FIXME: Should be better
// This just adds a slash to the start of the rootznode option that comes from the
// command line switch or default.
func setRootZnodeName (conf *bloorConfig, s string) {
	rootZnodeName := fmt.Sprintf("/%s", s)
	conf.rootZnode = rootZnodeName
}

func setConns (conf *bloorConfig) {
	// Setup sessions/connections
	for i := range conf.zkServers {
		// zk.Connect expects an array
		s := make([]string, 1)
		s[0] = conf.zkServers[i]
		conn, _, err := zk.Connect(s, time.Second)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Connected to %d", i)
		}
		conf.conns[i] = conn
	}
}

// FIXME: Should return something?
func createZkRootPath (conf *bloorConfig) {
	// Check if rootpath exists already on the first server.
	// If not, create it.
	exists, _, err := conf.conns[0].Exists(conf.rootZnode)
	if err != nil {
		log.Fatal(err)
	}

	if exists == true {
		log.Printf("Root znode %s already exists, not creating", conf.rootZnode)
	} else {
		rootZnodeContent := "bloor root znode"
		_, err := conf.conns[0].Create(conf.rootZnode, []byte(rootZnodeContent),
		conf.rootFlags, conf.acl)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Created znode root %s", conf.rootZnode)
		}
	}
}

func checkForChildren (conf *bloorConfig) {
	// Get children
	children, _, err := conf.conns[0].Children(conf.rootZnode)
	if err != nil {
		log.Fatal(err)
	}
	if len(children) > 0 {
		log.Fatalf("Children exist beneath root znode %s", conf.rootZnode)
		} else {
			log.Printf("Root znode %s has no children", conf.rootZnode)
		}
}

func createChildNodes (conf *bloorConfig) {
	// Create child nodes
	for i, conn := range conf.conns {
		childZnode := fmt.Sprintf("%s/session_%d", conf.rootZnode, i)
		childZnodeContent := fmt.Sprintf("child-%d", i)

		// First sync up
		_, err := conn.Sync(conf.rootZnode)
		if err != nil {
			log.Fatal(err)
		} else {
				log.Printf("Synced connection %d", i)
		}

		// Now create nodes
		_, err = conn.Create(childZnode, []byte(childZnodeContent),
		conf.childFlags, conf.acl)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Child znode %s was created", childZnode)
		}
	}
}

func syncConnections (conf *bloorConfig, conn *zk.Conn) {
	// Sync up again
	_, err := conn.Sync(conf.rootZnode)
	if err != nil {
		log.Fatal(err)
		} else {
			log.Printf("Synced connection")
		}
}

// FIXME: What am I returning here?
func getWatchers (conf *bloorConfig) []<-chan zk.Event {
	// Sync all the servers, check children, setup watchers
	watchers := make([]<-chan zk.Event, len(conf.conns))
	for i, conn := range conf.conns {

		// sync
		syncConnections(conf, conn)

		// Check if expected number of children
		children, _, err := conn.Children(conf.rootZnode)
		if err != nil {
			log.Fatal(err)
		}
		if len(children) != len(conf.conns) {
			log.Fatalf("Expected children: %d, found children: %d", len(children),
			len(conf.conns))
		} else {
			log.Printf("Found correct number of children (%d) in connection %d",
			len(conf.conns), i)
		}

		// Set watchers
		snapshots, _, events, err := conn.ChildrenW(conf.rootZnode)
		if err != nil {
			log.Fatalf("Error setting up watch %s", err)
		} else {
			log.Printf("Set watcher on rootpath %s", conf.rootZnode)
			// NOTE(curtis): Does this make sense?
			for j, v := range snapshots {
				log.Printf("Watching child %s/%s on session %d", conf.rootZnode, v, j)
			}
			watchers[i] = events
		}
	}
	return watchers
}

func deleteChildNodes (conf *bloorConfig) {
	// Delete the child znodes
	for i, conn := range conf.conns {
		childZnode := fmt.Sprintf("%s/session_%d", conf.rootZnode, i)
		conn.Delete(childZnode, -1)
	}
}

func checkWatchers (conf *bloorConfig, watchers []<-chan zk.Event) {
	// Check the watchers
	for i, event := range watchers {
		// Sync up
		_, err := conf.conns[i].Sync(conf.rootZnode)
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
}

func deleteRootPath (conf *bloorConfig) {
	// Sync first session to delete
	syncConnections(conf, conf.conns[0])
	// Delete rootpath/rootzode
	err := conf.conns[0].Delete(conf.rootZnode, -1)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Deleted rootpath %s", conf.rootZnode)
	}
}

func closeConnections (conf *bloorConfig) {
	// Finally close all connections
	// FIXME: Could be defer?
	for _, conn := range conf.conns {
		conn.Close()
	}
}

func main() {

	app := cli.NewApp()
	app.Name = "bloor"
	app.Version = "0.0.2"
	app.Usage = "Zookeeper smoketest tool"

	var rootZnodeOption string
	var verbose bool
	var zkServers string

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "servers, s",
			Value:       "",
			Usage:       "list of servers",
			Destination: &zkServers,
			},
		cli.StringFlag{
			Name:        "znode-root, r",
			Value:       "bloor-smoketest",
			Usage:       "Root name of znode",
			Destination: &rootZnodeOption,
		},
		cli.BoolFlag{
			Name:        "verbose, V",
			Usage:       "Print logs",
			Destination: &verbose,
		},
	}

	app.Action = func(c *cli.Context) {

		// Setup bloor config
		conf := &bloorConfig{}
		conf.verbose = verbose
		conf.acl = zk.WorldACL(zk.PermAll)
		conf.rootFlags = int32(0)
		// Children are ephemeral
		conf.childFlags = int32(zk.FlagEphemeral)
		// Log to stdout to startup
		log.SetOutput(os.Stdout)
		log.Printf("Bloor smoketest started (use -V for more logging)")
		// Now only log if verbose is set
		if verbose == false {
			log.SetOutput(ioutil.Discard)
		}		

		setRootZnodeName(conf, rootZnodeOption)

		// Setup servers from environment variable or option
		if zkServers != "" {
			// zkServers comes in as a string, but want array in conf
			conf.zkServers = getServerArray(zkServers)
		} else {
			zksStr := os.Getenv("ZOOKEEPER_SERVERS")
			if zksStr != "" {
				conf.zkServers = getServerArray(zksStr)
			} else {
				log.Fatal("ZOOKEEPER_SERVERS environment variable does not exist or is empty")
			}
		}

		// make connections
		// FIXME: Is this correct in terms of creating the conns array in the bloorConfig?
		conf.conns = make([]*zk.Conn, len(conf.zkServers))
		setConns(conf)

		// Do the work
		createZkRootPath(conf)
		checkForChildren(conf)
		createChildNodes(conf)
		watchers := getWatchers(conf)
		deleteChildNodes(conf)
		checkWatchers(conf, watchers)
		deleteRootPath(conf)
		closeConnections(conf)

		// Back to normal logging
		log.SetOutput(os.Stdout)
		log.Printf("Bloor run completed")
	}

	app.Run(os.Args)
}
