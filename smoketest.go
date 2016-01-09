package main

// Smoketest
func smoketest(conf *bloorConfig) {
	for _, s := range conf.zkServers {
		// Create children on each session
		for i := 0; i < s.numChildren; i++ {
			s.createChildNode()
		}
		s.setWatchers()
		s.deleteChildNodes()
		s.checkWatchers()
	}
}
