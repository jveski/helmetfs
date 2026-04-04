package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime)

	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "mount":
		doMount(os.Args[2:])
	case "unmount":
		doUnmount(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  helmetfs mount <source-dir> <mountpoint> --replica <path> [options]\n")
	fmt.Fprintf(os.Stderr, "  helmetfs unmount <mountpoint>\n")
	os.Exit(1)
}

func doMount(args []string) {
	if len(args) < 2 {
		usage()
	}

	sourceDir := args[0]
	mountpoint := args[1]
	var replicaDir string
	replWorkers := 4
	scrubTime := "01:00"
	noRemoteMkdir := false

	i := 2
	for i < len(args) {
		switch args[i] {
		case "--replica":
			i++
			if i >= len(args) {
				usage()
			}
			replicaDir = args[i]
		case "--replication-workers":
			i++
			if i >= len(args) {
				usage()
			}
			n, err := strconv.Atoi(args[i])
			if err != nil {
				log.Fatalf("invalid --replication-workers: %s", args[i])
			}
			replWorkers = n
		case "--scrub-time":
			i++
			if i >= len(args) {
				usage()
			}
			scrubTime = args[i]
		case "--no-remote-mkdir":
			noRemoteMkdir = true
		default:
			log.Fatalf("unknown option: %s", args[i])
		}
		i++
	}

	if replicaDir == "" {
		log.Fatal("--replica is required")
	}

	sourceDir, err := filepath.Abs(sourceDir)
	if err != nil {
		log.Fatalf("realpath source: %v", err)
	}
	sourceDir, err = filepath.EvalSymlinks(sourceDir)
	if err != nil {
		log.Fatalf("realpath source: %v", err)
	}

	mountpoint, err = filepath.Abs(mountpoint)
	if err != nil {
		log.Fatalf("realpath mountpoint: %v", err)
	}
	mountpoint, err = filepath.EvalSymlinks(mountpoint)
	if err != nil {
		log.Fatalf("realpath mountpoint: %v", err)
	}

	replicaDir, err = filepath.Abs(replicaDir)
	if err != nil {
		log.Fatalf("realpath replica: %v", err)
	}
	replicaDir, err = filepath.EvalSymlinks(replicaDir)
	if err != nil {
		log.Fatalf("realpath replica: %v", err)
	}

	scrubHour, scrubMinute := parseScrubTime(scrubTime)

	state := NewFsState(sourceDir, replicaDir, scrubHour, scrubMinute, replWorkers, noRemoteMkdir)
	initState(state)

	for i := 0; i < replWorkers; i++ {
		state.replWg.Add(1)
		go replWorkerLoop(state)
	}
	state.scrubWg.Add(1)
	go scrubLoop(state)

	rootNode, err := NewHelmetRoot(sourceDir, state)
	if err != nil {
		log.Fatalf("NewHelmetRoot: %v", err)
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			FsName:     "helmetfs",
			Name:       "helmetfs",
			MaxBackground: 10,
		},
		NullPermissions: true,
	}

	server, err := fs.Mount(mountpoint, rootNode, opts)
	if err != nil {
		log.Fatalf("mount failed: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		state.shutdown.Store(true)
		server.Unmount()
	}()

	log.Printf("helmetfs mounted: source=%s mountpoint=%s replica=%s", sourceDir, mountpoint, replicaDir)
	server.Wait()

	flushDirtyFiles(state)
	stopWorkers(state)
	log.Println("helmetfs: unmounted")
}

func doUnmount(args []string) {
	if len(args) < 1 {
		usage()
	}
	mountpoint := args[0]
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.Command("umount", mountpoint)
	} else {
		cmd = exec.Command("fusermount3", "-u", mountpoint)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("unmount failed: %v", err)
	}
}

func parseScrubTime(s string) (int, int) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		log.Fatalf("invalid scrub time: %s", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		log.Fatalf("invalid scrub hour: %s", parts[0])
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		log.Fatalf("invalid scrub minute: %s", parts[1])
	}
	return h, m
}
