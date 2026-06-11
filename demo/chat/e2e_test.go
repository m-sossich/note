package main_test

// TestChat_E2E spins up 1 bootstrap + 5 chat nodes. Each node starts knowing
// ONLY the bootstrap — not each other. The test asserts three properties in
// sequence:
//
//  1. Peer discovery: UDP ANNOUNCE → PEERS propagation connects every chat
//     node to all 4 other chat nodes (even though none of them were given the
//     others' addresses at startup).
//
//  2. ANNOUNCE exchange: once TCP connections exist, every node sends an
//     ANNOUNCE to its new peer. All 5 nodes must eventually see all 4 other
//     chat nodes in their room roster.
//
//  3. Message delivery: each of the 5 nodes broadcasts one unique message;
//     every other node must receive it with the correct text and
//     ANNOUNCE-verified sender name.
//
// Ports 19900–19905.

import (
	"fmt"
	"testing"
	"time"

	note "github.com/m-sossich/note"
	"github.com/m-sossich/note/demo/chat/protocol"
	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/node"
)

// receivedMsg is a message captured by the test callback.
type receivedMsg struct {
	fromUsername string
	text         string
}

// chatTestNode is a running chat peer wired for test observation.
type chatTestNode struct {
	id   string
	p    *note.Peer
	h    *protocol.Handler
	msgs chan receivedMsg
}

// newChatNode creates and starts a verified-mode chat node in the "test" room.
// It registers t.Cleanup to close the peer when the test ends.
func newChatNode(t *testing.T, addr, bootstrapAddr, username string) *chatTestNode {
	t.Helper()

	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("newChatNode %s: generate keypair: %v", addr, err)
	}

	cn := &chatTestNode{
		msgs: make(chan receivedMsg, 50),
	}

	var h *protocol.Handler

	p, err := note.NewVerifiedPeer(kp, addr,
		note.WithBootstrap(bootstrapAddr),
		note.WithHandlerFactory(func(n node.Node) {
			h = protocol.NewHandler(n, "test", username, kp.PrivateKey)
			h.SetOnMessage(func(fromUsername, text string) {
				select {
				case cn.msgs <- receivedMsg{fromUsername, text}:
				default:
				}
			})
		}),
		note.WithPeerConnected(func(peerID string) { h.OnConnect(peerID) }),
		note.WithPeerDisconnected(func(peerID string) { h.OnDisconnect(peerID) }),
	)
	if err != nil {
		t.Fatalf("newChatNode %s: start peer: %v", addr, err)
	}
	t.Cleanup(func() { p.Close() })

	cn.p = p
	cn.h = h
	cn.id = p.ID()
	return cn
}

func TestChat_E2E(t *testing.T) {
	const (
		bootstrapAddr = "127.0.0.1:19900"
		addrAlice     = "127.0.0.1:19901"
		addrBob       = "127.0.0.1:19902"
		addrCarol     = "127.0.0.1:19903"
		addrDave      = "127.0.0.1:19904"
		addrEve       = "127.0.0.1:19905"
	)

	// ---- bootstrap ---- (discovery-only; no chat handler)
	bsKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("bootstrap keypair: %v", err)
	}
	bs, err := note.NewVerifiedPeer(bsKP, bootstrapAddr)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	// ---- 5 chat nodes — each only knows the bootstrap at startup ----
	usernames := []string{"alice", "bob", "carol", "dave", "eve"}
	addrs := []string{addrAlice, addrBob, addrCarol, addrDave, addrEve}

	nodes := make([]*chatTestNode, len(usernames))
	for i, username := range usernames {
		nodes[i] = newChatNode(t, addrs[i], bootstrapAddr, username)
	}

	// Build the set of chat peer IDs for membership assertions.
	chatIDs := make([]string, len(nodes))
	for i, n := range nodes {
		chatIDs[i] = n.id
	}

	// ---- phase 1: peer discovery + ANNOUNCE exchange ----
	//
	// Each node announces itself to the bootstrap over UDP. The bootstrap
	// replies to every newcomer with its full peer list. Each node then dials
	// the discovered peers over TCP and immediately sends a signed ANNOUNCE.
	//
	// We combine both checks into a single poll: each node must have all 4
	// other chat peers in its TCP connection table (peer discovery) AND in
	// its room roster (ANNOUNCE received and verified). Keeping them together
	// eliminates the race where phase 1 passes with a connection that was
	// established so recently that its ANNOUNCE is still in-flight.
	t.Log("phase 1: waiting for peer discovery and ANNOUNCE exchange")
	waitUntil(t, 30*time.Second, 200*time.Millisecond, func() (bool, string) {
		for i, n := range nodes {
			peerSet := make(map[string]struct{})
			for _, id := range n.p.Peers() {
				peerSet[id] = struct{}{}
			}
			for j, id := range chatIDs {
				if j == i {
					continue
				}
				if _, ok := peerSet[id]; !ok {
					return false, fmt.Sprintf("%s missing peer %s", usernames[i], usernames[j])
				}
			}
			if got := len(n.h.Members()); got < 4 {
				return false, fmt.Sprintf("%s has %d/4 members", usernames[i], got)
			}
		}
		return true, ""
	})
	t.Log("phase 1 complete: all nodes connected and ANNOUNCE exchanged")

	// ---- phase 2: message delivery ----
	//
	// Each node broadcasts one unique message to all connected peers. We
	// verify that every other chat node receives the correct text, attributed
	// to the correct ANNOUNCE-verified sender name.
	t.Log("phase 2: verifying message delivery")
	for i, sender := range nodes {
		text := fmt.Sprintf("hello from %s", usernames[i])
		sender.p.Broadcast(note.Msg(protocol.Protocol, protocol.MsgMessage, protocol.ChatMessage{
			Room:     "test",
			Username: usernames[i],
			Text:     text,
		}))

		for j, receiver := range nodes {
			if j == i {
				continue
			}
			select {
			case msg := <-receiver.msgs:
				if msg.text != text {
					t.Errorf("%s received wrong text from %s: got %q, want %q",
						usernames[j], usernames[i], msg.text, text)
				}
				if msg.fromUsername != usernames[i] {
					t.Errorf("%s received wrong sender: got %q, want %q",
						usernames[j], msg.fromUsername, usernames[i])
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s did not receive message from %s within 5s", usernames[j], usernames[i])
			}
		}
	}
	t.Log("phase 2 complete: all messages delivered and verified")
}

// waitUntil polls cond every poll interval until it returns true or timeout
// expires. On timeout it calls t.Fatalf with the last diagnostic string.
func waitUntil(t *testing.T, timeout, poll time.Duration, cond func() (bool, string)) {
	t.Helper()
	deadline := time.After(timeout)
	var last string
	for {
		if ok, _ := cond(); ok {
			return
		}
		select {
		case <-deadline:
			_, last = cond()
			t.Fatalf("timed out after %s: %s", timeout, last)
		case <-time.After(poll):
		}
	}
}
