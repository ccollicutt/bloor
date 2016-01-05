# bloor: test zookeeper systems

Right now bloor is just a Go version of [zk-smoketest](https://github.com/phunt/zk-smoketest) (which is in Python). I hope to add more functionality to this project in the near future. It's meant to be used just to verify that a Zookeeper cluster is up and running and that a few things are working.

Currently it reads ZOOKEEPER_SERVERS from an environment variable, connects to each server, creates a root path, then children, sets up watchers, deletes the children, and finally checks to see what the watchers report. That's it.

```shell
$ # assuming these are your zookeeper servers
$ export ZOOKEEPER_SERVERS="192.168.0.2:2181,192.168.0.3:2181,192.168.4:2181"
$ bloor
2016/01/04 15:06:58 Bloor smoketest started (use -V for more logging)
2016/01/04 15:06:58 Bloor run completed
```

It's named after Bloor street in Toronto.
