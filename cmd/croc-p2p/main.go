// Command croc-p2p is the CLI entrypoint for the peer-to-peer file sharing
// tool built on the croc relay control plane and a direct N-N data plane.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/huykhanh2141/croc-p2p/internal/chunker"
	"github.com/huykhanh2141/croc-p2p/internal/room"
	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

const usageText = `croc-p2p - P2P N-N file sharing over the croc relay

Usage:
  croc-p2p create --room <id> [--secret <secret>] [--relay <addr>]
  croc-p2p join   --room <id> [--secret <secret>] [--relay <addr>]
  croc-p2p share  <file>
  croc-p2p get    <file>
  croc-p2p version

Commands:
  create   Create a room and become its first coordinator.
  join     Join an existing room via the relay and PAKE handshake.
  share    Prepare a file, chunk it (512 KiB) and announce it.
  get      Download an announced file from the swarm.
  version  Print the protocol version.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "create":
		err = runCreate(os.Args[2:])
	case "join":
		err = runJoin(os.Args[2:])
	case "share":
		err = runShare(os.Args[2:])
	case "get":
		err = runGet(os.Args[2:])
	case "version":
		fmt.Printf("croc-p2p %s\n", protocol.ProtocolVersion)
	case "help", "-h", "--help":
		fmt.Print(usageText)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "croc-p2p: %v\n", err)
		os.Exit(1)
	}
}

func runCreate(args []string) error {
	flags := flag.NewFlagSet("create", flag.ContinueOnError)
	roomID := flags.String("room", "", "room identifier")
	secret := flags.String("secret", "", "shared room secret used for PAKE")
	relay := flags.String("relay", protocol.DefaultRelay, "croc relay address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *roomID == "" {
		return fmt.Errorf("create: --room is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	manager := room.New(*relay)
	defer manager.Close()

	peer, err := manager.CreateRoom(ctx, *roomID, *secret)
	if err != nil {
		return err
	}

	fmt.Printf("room %q created\n", *roomID)
	fmt.Printf("coordinator: %s (%s)\n", peer.Name, peer.ID)
	// TODO(member-3): show the PEER_LIST and file announcements in a live UI.
	fmt.Println("waiting for peers; press Ctrl+C to stop")

	<-ctx.Done()
	fmt.Println("\nleaving room")
	return manager.LeaveRoom(context.Background())
}

func runJoin(args []string) error {
	flags := flag.NewFlagSet("join", flag.ContinueOnError)
	roomID := flags.String("room", "", "room identifier")
	secret := flags.String("secret", "", "shared room secret used for PAKE")
	relay := flags.String("relay", protocol.DefaultRelay, "croc relay address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *roomID == "" {
		return fmt.Errorf("join: --room is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	manager := room.New(*relay)
	defer manager.Close()

	peer, peers, err := manager.JoinRoom(ctx, *roomID, *secret)
	if err != nil {
		return err
	}

	fmt.Printf("joined room %q as %s (%s)\n", *roomID, peer.Name, peer.ID)
	fmt.Printf("peers (%d):\n", len(peers))
	for _, member := range peers {
		role := "member"
		if member.Coordinator {
			role = "coordinator"
		}
		fmt.Printf("  - %s (%s) [%s]\n", member.Name, member.ID, role)
	}
	fmt.Println("waiting for files; press Ctrl+C to stop")

	<-ctx.Done()
	fmt.Println("\nleaving room")
	return manager.LeaveRoom(context.Background())
}

func runShare(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("share: <file> is required")
	}
	path := args[0]

	manager := chunker.New()
	meta, err := manager.PrepareFile(path)
	if err != nil {
		return err
	}

	fmt.Printf("prepared %s\n", meta.Name)
	fmt.Printf("  size:      %d bytes\n", meta.Size)
	fmt.Printf("  chunks:    %d x %d bytes\n", meta.NumChunks, meta.ChunkSize)
	fmt.Printf("  sha256:    %s\n", meta.SHA256)
	fmt.Println("TODO: announce FILE_ANNOUNCE to the room and serve ChunkData requests")
	return nil
}

func runGet(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("get: <file> is required")
	}
	fmt.Printf("requested %s\n", args[0])
	fmt.Println("TODO: resolve FileMeta, read BITFIELD_UPDATE, and start the multi-source swarm download")
	return nil
}
