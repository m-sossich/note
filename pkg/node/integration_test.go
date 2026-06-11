package node_test

import (
	"testing"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/node/identify"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

func TestNode_GracefulStop(t *testing.T) {
	jc := jsoncdc.New()
	n, err := node.New(node.Config{
		NodeID:     "node-A",
		ListenAddr: "127.0.0.1:19400",
		Transport:  tcptransport.New(0),
		Handshaker: identify.New(identify.Config{}),
		Codec:      jc,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if err := n.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}
