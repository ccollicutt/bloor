package main

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
	zkServersStr string
	zkServers    []string
	rootZnode    string
	verbose      bool
	acl          []zk.ACL
	rootFlags    int32
	childFlags   int32
	conns        []*zk.Conn
}

func newBloorConfig() *bloorConfig {
	conf := &bloorConfig{
		zkServersStr: "127.0.0.1",
		verbose:      false,
		acl:          zk.WorldACL(zk.PermAll),
		rootFlags:    int32(0),
		childFlags:   int32(zk.FlagEphemeral),
		rootZnode:    "/bloor-smoketest",
	}
	return conf
}

func (conf *bloorConfig) setServerArray() {
	conf.zkServers = strings.Split(conf.zkServersStr, ",")
	for i, v := range conf.zkServers {
		log.Printf("Server %d: %s", i, v)
	}
}

func (conf *bloorConfig) setConns() {
	// Setup sessions/connections
	conf.conns = make([]*zk.Conn, len(conf.zkServers))
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
func (conf *bloorConfig) createZkRootPath() {
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

func (conf *bloorConfig) checkForExistingChildren() bool {
	// Get children
	children, _, err := conf.conns[0].Children(conf.rootZnode)
	if err != nil {
		log.Fatal(err)
	}
	if len(children) > 0 {
		log.Printf("Children exist beneath root znode %s", conf.rootZnode)
		return true
	}

	log.Printf("Root znode %s has no children", conf.rootZnode)
	return false
}

func (conf *bloorConfig) createChildNodes() {
	// Sync
	conf.syncConnections()
	// Create child nodes
	for i, conn := range conf.conns {
		childZnode := fmt.Sprintf("%s/session_%d", conf.rootZnode, i)
		childZnodeContent := fmt.Sprintf("child-%d", i)

		// Now create nodes
		_, err := conn.Create(childZnode, []byte(childZnodeContent),
			conf.childFlags, conf.acl)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Child znode %s was created", childZnode)
		}
	}
}

func (conf *bloorConfig) syncConnections() {
	for _, conn := range conf.conns {
		_, err := conn.Sync(conf.rootZnode)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Synced connections")
		}
	}
}

// FIXME: What am I returning here?
func getWatchers(conf *bloorConfig) []<-chan zk.Event {
	// Sync all the servers, check children, setup watchers
	watchers := make([]<-chan zk.Event, len(conf.conns))
	conf.syncConnections()
	for i, conn := range conf.conns {

		// sync

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

func (conf *bloorConfig) deleteChildNodes() {
	// Delete the child znodes
	for i, conn := range conf.conns {
		childZnode := fmt.Sprintf("%s/session_%d", conf.rootZnode, i)
		conn.Delete(childZnode, -1)
	}
}

func checkWatchers(conf *bloorConfig, watchers []<-chan zk.Event) {
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

func (conf *bloorConfig) deleteRootPath() {
	// Sync first session to delete
	conf.syncConnections()
	// Delete rootpath/rootzode
	err := conf.conns[0].Delete(conf.rootZnode, -1)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Deleted rootpath %s", conf.rootZnode)
	}
}

func (conf *bloorConfig) closeConnections() {
	// Finally close all connections
	// FIXME: Could be defer?
	for _, conn := range conf.conns {
		conn.Close()
	}
}

func main() {

	app := cli.NewApp()
	app.Name = "bloor"
	app.Version = "0.0.3"
	app.Usage = "Zookeeper smoketest tool"

	var rootZnodeOption string
	var verbose bool
	var zkServersOption string

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "servers, s",
			Value:       "",
			Usage:       "list of servers",
			Destination: &zkServersOption,
		},
		cli.StringFlag{
			Name:        "znode-root, r",
			Value:       "",
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

		// Log to stdout to startup
		log.SetOutput(os.Stdout)
		log.Printf("Bloor smoketest started (use -V for more logging)")

		// create new bloor
		conf := newBloorConfig()

		// Now only log if verbose is set
		if verbose == false {
			log.SetOutput(ioutil.Discard)
		} else {
			conf.verbose = verbose
		}

		// Set rootZnode
		// NOTE(curtis): Am setting default name in newBloorConfig, not sure where
		// to do it...
		if rootZnodeOption != "" {
			hasSlash := strings.HasPrefix(rootZnodeOption, "/")
			if hasSlash {
				conf.rootZnode = rootZnodeOption
			} else {
				// Add slash to front of string
				conf.rootZnode = fmt.Sprintf("/%s", rootZnodeOption)
			}
		}

		// Setup servers from environment variable or option
		if zkServersOption != "" {
			// zkServers comes in as a string, but want array in conf
			conf.zkServersStr = zkServersOption
		} else {
			zkServersEnvironment := os.Getenv("ZOOKEEPER_SERVERS")
			if zkServersEnvironment != "" {
				conf.zkServersStr = zkServersEnvironment
			} else {
				log.Fatal("ZOOKEEPER_SERVERS environment variable does not exist or is empty")
			}
		}

		// Now that we have set the server string, we can create the array
		conf.setServerArray()

		// make connections
		// NOTE(curtis): I'd like to set this when newBloorConfig is runn but don't
		// know how many servers there are until it's set.
		conf.setConns()

		// Do the work
		conf.createZkRootPath()

		// See if root has existing children
		existing := conf.checkForExistingChildren()
		if existing {
			log.Fatalf("Existing children, exiting")
		}
		conf.createChildNodes()
		watchers := getWatchers(conf)
		conf.deleteChildNodes()
		checkWatchers(conf, watchers)
		conf.deleteRootPath()
		conf.closeConnections()

		// Back to normal logging
		log.SetOutput(os.Stdout)
		log.Printf("Bloor run completed")
	}

	app.Run(os.Args)
}
