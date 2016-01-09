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
	zkServers []zkServer
	verbose   bool
}

type zkServer struct {
	rootZnode  string
	acl        []zk.ACL
	rootFlags  int32
	childFlags int32
	server     string
	conn       *zk.Conn
	// No ideas!!!
	watchers    <-chan zk.Event
	numChildren int
}

func newBloorConfig() *bloorConfig {
	conf := &bloorConfig{
		verbose: false,
	}
	return conf
}

func newZkServer(name string, numZnodes int, rootZnodeOption string) *zkServer {
	// zk.Connect expects an array
	s := make([]string, 1)
	s[0] = name
	conn, _, err := zk.Connect(s, time.Second)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Connected to %s", name)
	}

	server := &zkServer{
		server:      name,
		conn:        conn,
		acl:         zk.WorldACL(zk.PermAll),
		rootFlags:   int32(0),
		childFlags:  int32(zk.FlagEphemeral),
		rootZnode:   rootZnodeOption,
		numChildren: numZnodes,
	}
	return server
}

func setServerArray(zkServersStr string) []string {
	serverArray := strings.Split(zkServersStr, ",")
	for i, v := range serverArray {
		log.Printf("Server %d: %s", i, v)
	}
	return serverArray
}

// FIXME: Should return something?
func (server *zkServer) createZkRootPath() {
	// Check if rootpath exists already on the first server.
	// If not, create it.
	exists, _, err := server.conn.Exists(server.rootZnode)
	if err != nil {
		log.Fatal(err)
	}

	if exists == true {
		log.Printf("Root znode %s already exists, not creating", server.rootZnode)
	} else {
		rootZnodeContent := "bloor root znode"
		_, err := server.conn.Create(server.rootZnode, []byte(rootZnodeContent),
			server.rootFlags, server.acl)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Created znode root %s", server.rootZnode)
		}
	}
}

func (server *zkServer) checkForExistingChildren() bool {
	// Get children
	children, _, err := server.conn.Children(server.rootZnode)
	if err != nil {
		log.Fatal(err)
	}
	if len(children) > 0 {
		log.Printf("Children exist beneath root znode %s", server.rootZnode)
		return true
	}

	log.Printf("Root znode %s has no children", server.rootZnode)
	return false
}

func (server *zkServer) syncConnection() {
	_, err := server.conn.Sync(server.rootZnode)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Synced connections")
	}
}

func (server *zkServer) createChildNode() {
	// Create child nodes
	childZnode := fmt.Sprintf("%s/child-", server.rootZnode)
	childZnodeContent := fmt.Sprintf("child-%s", server.server)

	// First sync up
	_, err := server.conn.Sync(server.rootZnode)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Synced connection %s", server.server)
	}

	// Now create nodes
	child, err := server.conn.CreateProtectedEphemeralSequential(childZnode,
		[]byte(childZnodeContent), server.acl)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Child znode %s was created", child)
	}
}

func (server *zkServer) setWatchers() {
	// Sync all the server, check children, setup watchers
	server.syncConnection()

	// Set watchers
	snapshots, _, events, err := server.conn.ChildrenW(server.rootZnode)
	if err != nil {
		log.Fatalf("Error setting up watch %s", err)
	} else {
		log.Printf("Set watcher on rootpath %s", server.rootZnode)
		// NOTE(curtis): Does this make sense?
		for _, child := range snapshots {
			log.Printf("Watching child %s/%s", server.rootZnode, child)
		}
		// No idea what is going on here!!!
		server.watchers = events
	}
}

// Delete all the child nodes for the server?
func (server *zkServer) deleteChildNodes() {
	children, _, err := server.conn.Children(server.rootZnode)
	if err != nil {
		log.Fatalf("Failed to get children on session %s", server.server)
	}

	if len(children) == 0 {
		log.Printf("Session %s has no children", server.server)
	}

	for _, child := range children {
		childPath := fmt.Sprintf("%s/%s", server.rootZnode, child)
		err := server.conn.Delete(childPath, -1)
		if err != nil {
			log.Fatalf("Failed to delete %s on session %s with error %s",
				childPath, server.server, err)
		} else {
			log.Printf("Deleted child %s on session %s", childPath, server.server)
		}
	}
}

func (server *zkServer) checkWatchers() {
	// Sync connection
	_, err := server.conn.Sync(server.rootZnode)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Synced connection %s", server.server)
	}

	//FIXME: Need to work on channels, we should be checkign how many events fired
	// and that it should be the same, I believe, as the number of deleted
	// children per session.
	i := 0
	for msg := range server.watchers {
		log.Printf("Event type %s occured on path %s with session %s",
			zk.EventType(msg.Type), msg.Path, server.server)
		i++
	}
	if i < 1 {
		log.Fatalf("Was expecting at least 1 watcher event")
	} else {
		log.Printf("Found at least 1 watcher event")
	}

}

func (server zkServer) deleteRootPath() {
	// Sync first session to delete
	server.syncConnection()
	// Delete rootpath/rootzode
	err := server.conn.Delete(server.rootZnode, -1)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Deleted rootpath %s", server.rootZnode)
	}
}

func main() {

	app := cli.NewApp()
	app.Name = "bloor"
	app.Version = "0.0.4"
	app.Usage = "Zookeeper smoketest tool"

	var rootZnodeOption string
	var verbose bool
	var zkServersOption string
	var numZnodesOption int

	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:        "znodes, z",
			Value:       1,
			Usage:       "number of znode children to create on each server",
			Destination: &numZnodesOption,
		},
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
			if hasSlash == false {
				rootZnodeOption = fmt.Sprintf("/%s", rootZnodeOption)
			}
		}

		// If not set by -s then get from environment variable
		if zkServersOption == "" {
			zkServersOption = os.Getenv("ZOOKEEPER_SERVERS")
			if zkServersOption == "" {
				log.Fatal("ZOOKEEPER_SERVERS environment variable does not exist or is empty")
			}
		}

		// Now that we have set the server string, we can create the array
		zkServersArray := setServerArray(zkServersOption)
		conf.zkServers = make([]zkServer, len(zkServersArray))

		// make connections
		// NOTE(curtis): I'd like to set this when newBloorConfig is runn but don't
		// know how many servers there are until it's set.
		for i, s := range zkServersArray {
			server := newZkServer(s, numZnodesOption, rootZnodeOption)
			conf.zkServers[i] = *server
		}

		// Create the rootpath on the first server
		conf.zkServers[0].createZkRootPath()
		existing := conf.zkServers[0].checkForExistingChildren()
		if existing {
			log.Fatalf("Existing children, exiting")
		}

		// Actually run smoketest
		smoketest(conf)

		// Delete rootpath via first server
		conf.zkServers[0].deleteRootPath()

		// Close connections
		for _, s := range conf.zkServers {
			s.conn.Close()
		}

		// Back to normal logging
		log.SetOutput(os.Stdout)
		log.Printf("Bloor run completed")
	}

	app.Run(os.Args)
}
