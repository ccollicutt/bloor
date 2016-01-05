package main

import (
  "testing"
  "fmt"
)

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

func TestGetServerMultiArray(t *testing.T) {
  serverStr := "192.168.0.2:2181,192.168.0.3:2181,192.168.0.4:2181"
  var servers []string
  servers = getServerArray(serverStr)
  serversExpected := []string{
      "192.168.0.2:2181",
      "192.168.0.3:2181",
      "192.168.0.4:2181",
  }

  if len(servers) != 3 {
    t.Error("Expected server array to be length of 3")
  }

  ok := StringArrayEquals(servers, serversExpected)
  if ok == false {
    t.Error("Server array is not the same as the serverExpected array")
  }
}

func TestGetServerSingleArray(t *testing.T) {
  serverStr := "192.168.0.2"
  var servers []string
  servers = getServerArray(serverStr)
  serversExpected := []string{
    "192.168.0.2",
  }

  if len(servers) != 1 {
    t.Error("Expected server array to be length of 3")
  }

  ok := StringArrayEquals(servers, serversExpected)
  if ok != true {
    t.Error("Server array is not the same as the serverExpected array")
  }
}

func TestSetRootZnodeName(t *testing.T) {
  rootZnodeOption := "someznode"
  rootZnode := fmt.Sprintf("/%s", rootZnodeOption)
  testConf := &bloorConfig{}
  setRootZnodeName(testConf, rootZnodeOption)
  if testConf.rootZnode != rootZnode {
    t.Errorf("Expected root znode is %s but instead is %s", rootZnode,
      testConf.rootZnode)
  }
}

func TestSetConns(t *testing.T) {
    // FIXME: need to figure out how to do this
}
