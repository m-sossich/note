// demo/gossip demonstrates epidemic broadcast: each node publishes one message
// and the gossip protocol ensures every node eventually receives every message,
// with no central coordinator. Hop counts show how many relays a message
// traversed before reaching the receiver.
//
// Modes:
//
//	bootstrap — run a discovery bootstrap node
//	node      — join the network, publish a message, and print received messages
//
// Usage:
//
//	# Terminal 1 — bootstrap
//	go run ./demo/gossip --mode bootstrap --addr 127.0.0.1:6000
//
//	# Terminal 2..N — one message per node
//	go run ./demo/gossip --bootstrap 127.0.0.1:6000 --message "hello from alice"
//	go run ./demo/gossip --bootstrap 127.0.0.1:6000 --message "hello from bob"
//	go run ./demo/gossip --bootstrap 127.0.0.1:6000 --message "hello from carol"
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	note "github.com/m-sossich/note"
	"github.com/m-sossich/note/demo/gossip/protocol"
	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/node"
)

const (
	// publishDelay gives newly-connected peers time to establish their own
	// connections so the initial broadcast reaches a fuller mesh.
	publishDelay = 3 * time.Second
)

func main() {
	mode := flag.String("mode", "node", "bootstrap | node")
	addr := flag.String("addr", "0.0.0.0:6000", "UDP discovery + TCP listen address")
	bootstrapFlag := flag.String("bootstrap", "", "bootstrap node address")
	message := flag.String("message", "", "[node] message to publish after connecting")
	advertiseAddr := flag.String("advertise-addr", "", "public IP:port for NAT")
	trusted := flag.Bool("trusted", false, "disable verified mode")
	idPath := flag.String("id", "", "identity file path")
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	flag.Parse()

	setupLogging(*logLevel)

	var bootstraps []string
	if *bootstrapFlag != "" {
		bootstraps = strings.Split(*bootstrapFlag, ",")
	}

	switch *mode {
	case "bootstrap":
		runBootstrap(*addr, *idPath, bootstraps, *trusted)
	case "node":
		if *message == "" {
			slog.Error("--message is required in node mode")
			os.Exit(1)
		}
		runNode(*addr, *idPath, bootstraps, *advertiseAddr, *message, *trusted)
	default:
		slog.Error("unknown mode", "mode", *mode)
		os.Exit(1)
	}
}

func runBootstrap(addr, idPath string, bootstraps []string, trusted bool) {
	var (
		p   *note.Peer
		err error
	)
	opts := []note.Option{note.WithBootstrap(bootstraps...)}
	if trusted {
		opts = append(opts, note.WithIdentityPath(resolveIDPath(idPath, addr)))
		p, err = note.NewPeer(addr, opts...)
	} else {
		kp := mustKeypair(idPath, addr)
		p, err = note.NewVerifiedPeer(kp, addr, opts...)
	}
	if err != nil {
		slog.Error("start bootstrap", "err", err)
		os.Exit(1)
	}
	defer p.Close()
	fmt.Printf("[bootstrap] ready on %s\n", p.Addr())
	waitSignal()
}

func runNode(addr, idPath string, bootstraps []string, advertiseAddr, message string, trusted bool) {
	var h *protocol.Handler

	opts := []note.Option{note.WithBootstrap(bootstraps...)}
	if advertiseAddr != "" {
		opts = append(opts, note.WithAdvertiseAddr(advertiseAddr))
	}

	var (
		p      *note.Peer
		err    error
		nodeID string
	)

	if trusted {
		// Resolve the UUID before the factory so the handler has the correct
		// OriginID. LoadOrGenerateID reads/creates the same file NewPeer uses.
		nodeID, err = note.LoadOrGenerateID(resolveIDPath(idPath, addr))
		if err != nil {
			slog.Error("load identity", "err", err)
			os.Exit(1)
		}
		opts = append(opts,
			note.WithIdentityPath(resolveIDPath(idPath, addr)),
			note.WithHandlerFactory(func(n node.Node) {
				h = protocol.NewHandler(n, nodeID)
			}),
		)
		p, err = note.NewPeer(addr, opts...)
	} else {
		kp := mustKeypair(idPath, addr)
		nodeID = kp.NodeID
		opts = append(opts, note.WithHandlerFactory(func(n node.Node) {
			h = protocol.NewHandler(n, nodeID)
		}))
		p, err = note.NewVerifiedPeer(kp, addr, opts...)
	}
	if err != nil {
		slog.Error("start peer", "err", err)
		os.Exit(1)
	}
	defer p.Close()

	slog.Info("node started", "node_id", p.ID(), "addr", p.Addr())

	h.SetOnReceive(func(msg protocol.GossipMessage, senderID string) {
		if msg.OriginID == p.ID() {
			fmt.Printf("[published] %q  id=%s\n", msg.Text, msg.ID[:8])
		} else {
			fmt.Printf("[received]  %q  origin=%s  hops=%d\n", msg.Text, msg.OriginID[:8], msg.Hops)
		}
	})

	// Brief delay so the mesh has a chance to form before we originate.
	// Gossip is best-effort — if no peers are connected yet, the message
	// is published locally and nobody else receives it.
	time.Sleep(publishDelay)
	h.Publish(message)

	waitSignal()
	fmt.Println("[gossip] shutting down")
}

func mustKeypair(idPath, addr string) *identity.Keypair {
	kp, err := identity.LoadOrGenerate(resolveKeyPath(idPath, addr))
	if err != nil {
		slog.Error("load identity", "err", err)
		os.Exit(1)
	}
	return kp
}

func resolveKeyPath(idPath, addr string) string {
	if idPath != "" {
		return idPath
	}
	return "./" + sanitizeAddr(addr) + ".key"
}

func resolveIDPath(idPath, addr string) string {
	if idPath != "" {
		return idPath
	}
	return "./" + sanitizeAddr(addr) + ".id"
}

func sanitizeAddr(addr string) string {
	return strings.NewReplacer(":", "_", ".", "_").Replace(addr)
}

func waitSignal() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

func setupLogging(level string) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})))
}
