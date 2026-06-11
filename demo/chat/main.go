// demo/chat demonstrates a distributed P2P chat application built on the note
// framework. Nodes discover each other via a shared bootstrap, negotiate
// authenticated TCP connections, and exchange messages directly — no central
// server routes or stores any messages.
//
// Modes:
//
//	bootstrap  — run a public discovery bootstrap node (deploy on a VPS)
//	join       — join a chat room and start exchanging messages
//
// Usage:
//
//	# Start a bootstrap node (Terminal 1)
//	go run ./demo/chat --mode bootstrap --addr 0.0.0.0:8000
//
//	# Join the chat (Terminal 2)
//	go run ./demo/chat --bootstrap 127.0.0.1:8000 --username alice --room general
//
//	# Join from another terminal (Terminal 3)
//	go run ./demo/chat --bootstrap 127.0.0.1:8000 --username bob --room general
//
// Over the real internet, replace 127.0.0.1 with the bootstrap's public IP.
// If your machine is behind NAT, set --advertise-addr to your public IP:port.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	note "github.com/m-sossich/note"
	"github.com/m-sossich/note/demo/chat/protocol"
	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/node"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	mode := flag.String("mode", "join", "node mode: bootstrap | join")
	addr := flag.String("addr", "0.0.0.0:8000", "UDP discovery + TCP listen address")
	bootstrapFlag := flag.String("bootstrap", "", "bootstrap node address")
	room := flag.String("room", "general", "chat room name")
	username := flag.String("username", "", "display name (required in join mode)")
	advertiseAddr := flag.String("advertise-addr", "", "public IP:port announced to peers (required behind NAT)")
	trusted := flag.Bool("trusted", false, "disable verified mode (no TLS, no Ed25519 identity)")
	idPath := flag.String("id", "", "identity file path (auto-generated on first run if absent)")
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	flag.Parse()

	setupLogging(*logLevel)

	var bootstrapAddrs []string
	if *bootstrapFlag != "" {
		bootstrapAddrs = strings.Split(*bootstrapFlag, ",")
	}

	switch *mode {
	case "bootstrap":
		runBootstrap(*addr, *idPath, bootstrapAddrs, *advertiseAddr, *trusted)
	case "join":
		if *username == "" {
			slog.Error("--username is required in join mode")
			os.Exit(1)
		}
		runJoin(*addr, *idPath, bootstrapAddrs, *advertiseAddr, *room, *username, *trusted)
	default:
		slog.Error("unknown mode", "mode", *mode)
		os.Exit(1)
	}
}

// runBootstrap starts a minimal discovery entry point. It registers no
// application protocols — with capability filtering, chat nodes never route
// chat/1.0 or dht/1.0 traffic through it.
func runBootstrap(addr, idPath string, bootstrapAddrs []string, advertiseAddr string, trusted bool) {
	opts := []note.Option{note.WithBootstrap(bootstrapAddrs...)}
	if advertiseAddr != "" {
		opts = append(opts, note.WithAdvertiseAddr(advertiseAddr))
	}
	var p *note.Peer
	var err error
	if trusted {
		opts = append(opts, note.WithIdentityPath(resolveKeyPath(idPath, addr)))
		p, err = note.NewPeer(addr, opts...)
	} else {
		kp, kpErr := identity.LoadOrGenerate(resolveKeyPath(idPath, addr))
		if kpErr != nil {
			slog.Error("load identity", "err", kpErr)
			os.Exit(1)
		}
		p, err = note.NewVerifiedPeer(kp, addr, opts...)
	}
	if err != nil {
		slog.Error("start bootstrap", "err", err)
		os.Exit(1)
	}
	defer p.Close()
	slog.Info("bootstrap ready", "node_id", p.ID(), "addr", p.Addr())
	fmt.Printf("[bootstrap] ready on %s — share this address with your peers\n", p.Addr())
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("[bootstrap] shutting down")
}

func runJoin(addr, idPath string, bootstrapAddrs []string, advertiseAddr, room, username string, trusted bool) {
	var h *protocol.Handler
	opts := []note.Option{
		note.WithBootstrap(bootstrapAddrs...),
		note.WithDHT(),
	}
	if advertiseAddr != "" {
		opts = append(opts, note.WithAdvertiseAddr(advertiseAddr))
	}

	var p *note.Peer
	var err error
	if trusted {
		opts = append(opts,
			note.WithIdentityPath(resolveKeyPath(idPath, addr)),
			note.WithHandlerFactory(func(n node.Node) {
				h = protocol.NewHandler(n, room, username, nil)
			}),
			note.WithPeerConnected(func(peerID string) { h.OnConnect(peerID) }),
			note.WithPeerDisconnected(func(peerID string) { h.OnDisconnect(peerID) }),
		)
		p, err = note.NewPeer(addr, opts...)
	} else {
		kp, kpErr := identity.LoadOrGenerate(resolveKeyPath(idPath, addr))
		if kpErr != nil {
			slog.Error("load identity", "err", kpErr)
			os.Exit(1)
		}
		opts = append(opts,
			note.WithHandlerFactory(func(n node.Node) {
				h = protocol.NewHandler(n, room, username, kp.PrivateKey)
			}),
			note.WithPeerConnected(func(peerID string) { h.OnConnect(peerID) }),
			note.WithPeerDisconnected(func(peerID string) { h.OnDisconnect(peerID) }),
		)
		p, err = note.NewVerifiedPeer(kp, addr, opts...)
	}
	if err != nil {
		slog.Error("start peer", "err", err)
		os.Exit(1)
	}
	defer p.Close()
	slog.Info("node started", "node_id", p.ID(), "addr", p.Addr(), "verified", !trusted)
	doJoin(p, h, room, username)
}

func doJoin(p *note.Peer, h *protocol.Handler, room, username string) {
	fmt.Printf("--- joining room #%s as %s\n", room, username)
	fmt.Printf("--- node id: %s\n", p.ID())
	fmt.Printf("--- type a message and press Enter (messages go to whoever is connected)\n")
	fmt.Printf("--- /members to list room members, /quit to exit\n")

	// Announce room membership in DHT after a brief settling delay so the
	// routing table has a chance to populate. Peers already connected will
	// have received an ANNOUNCE via OnPeerConnected; this makes us
	// discoverable by peers who join later.
	go func() {
		ctx := context.Background()
		res, err := p.Announce(ctx, roomKey(room), []byte(username))
		if err != nil {
			slog.Warn("DHT announce failed", "err", err)
		} else if res.Attempted > 0 && res.Replicated == 0 {
			slog.Warn("DHT announce: no peers replicated", "room", room, "attempted", res.Attempted)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go readStdin(p, h, room, username, quit)

	<-quit
	fmt.Printf("\n--- %s left the room\n", username)
}

func readStdin(p *note.Peer, h *protocol.Handler, room, username string, quit chan<- os.Signal) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		switch text {
		case "/members":
			members := h.Members()
			if len(members) == 0 {
				fmt.Println("--- no other members in the room yet")
			} else {
				fmt.Printf("--- %d member(s) in #%s:\n", len(members), room)
				for _, name := range members {
					fmt.Printf("    %s\n", name)
				}
			}
		case "/quit":
			quit <- syscall.SIGTERM
			return
		default:
			p.Broadcast(note.Msg(protocol.Protocol, protocol.MsgMessage, protocol.ChatMessage{
				Room:     room,
				Username: username,
				Text:     text,
			}))
		}
	}
}

func roomKey(room string) []byte {
	return []byte("room:" + room)
}

func resolveKeyPath(idPath, addr string) string {
	if idPath != "" {
		return idPath
	}
	return "./" + sanitizeAddr(addr) + ".key"
}

func sanitizeAddr(addr string) string {
	return strings.NewReplacer(":", "_", ".", "_").Replace(addr)
}

func setupLogging(level string) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})))
}
