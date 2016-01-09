package main

import "testing"

// Borrowed from
// https://stackoverflow.com/questions/18561219/comparing-arrays-in-go-language
func StringArrayEquals(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestSetServerSingleArray(t *testing.T) {
	conf := newBloorConfig()
	conf.zkServersStr = "192.168.0.2:2181,192.168.0.3:2181,192.168.0.4:2181"
	conf.setServerArray()
	serversExpected := []string{
		"192.168.0.2:2181",
		"192.168.0.3:2181",
		"192.168.0.4:2181",
	}

	if len(conf.zkServers) != len(serversExpected) {
		t.Error("Expected server array to be length of %d", len(serversExpected))
	}

	ok := StringArrayEquals(conf.zkServers, serversExpected)
	if ok != true {
		t.Error("Server array is not the same as the serverExpected array")
	}
}
