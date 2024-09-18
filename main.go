// ttrpcstress is a simple client/server utility to stress-test a TTRPC connection.
// The server end simply listens for connections, has a single simple method exposed,
// and responds immediately to any requests. The client end spins up a number of worker
// goroutines, then has them send a number of requests to the server as fast as they can.
// The goal is to identify if there are deadlock cases with repeated quick TTRPC requests.
//
// Underlying facilities such as ttrpc.(*Client).Call and ttrpc.(*Server).Register are used,
// rather than generated TTRPC client/server code, to keep the test code simpler.
//
// The payload used for TTRPC operations here is a little complex. TTRPC package versions prior
// to v1.2.0 use gogoproto for encoding, which does not work with newer types generated via the
// protoc-gen-go tooling. To support both variants, we generate an identical protobuf (in subdirs
// "protogo" and "protogogo") using both "go" and "gogo" generators. Which payload type is used
// in the code is based on the presence of either the "protogo" or "protogogo" build tag.
// Effectively, this means you must pass "-tag protogogo" if building with ttrpc prior to v1.2.0.
// Otherwise, pass "-tag protogo".
//
// Suggested usage for ttrpcstress is to run the server, and the client with reasonable number of
// iterations and workers (perhaps 1,000,000 and 100, respectively), and observe that the client
// exits successfully (all requests completed and responses received) within some short timeframe.
//
// It is suggested that multiple versions of ttrpcstress be built, so that multiple versions of
// github.com/containerd/ttrpc can be tested, including mismatched versions between client/server.
// Some known issues in TTRPC package versions are as follows:
//   - A: Before v1.1.0: Original deadlock bug
//   - B: Between v1.1.0..v1.2.0: No known deadlock bugs
//     (fix in https://github.com/containerd/ttrpc/pull/94)
//   - C: Between v1.2.0..v1.2.4: Streaming with new deadlock bug
//     (introduced in https://github.com/containerd/ttrpc/pull/107)
//   - D: After v1.2.4: No known deadlock bugs
//     (fix in https://github.com/containerd/ttrpc/pull/168)
//
// If the client is in range A or C, it can enounter deadlock bugs. However, if the server is also
// in range C or D, the deadlock bug won't be hit. The reason C is included here is that prior to
// C, the server would stop receiving new requests if the client was not keeping up with responses
// (which is reasonable behavior for a server). Starting in C, the server will continue receiving
// requests even if the client is not reading responses fast enough.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/containerd/ttrpc"
	"golang.org/x/sync/errgroup"
)

func main() {
	flagHelp := flag.Bool("help", false, "Display usage")
	flag.Parse()
	if *flagHelp || flag.NArg() < 2 {
		usage()
	}
	switch flag.Arg(0) {
	case "server":
		if flag.NArg() != 2 {
			usage()
		}
		pipe := flag.Arg(1)
		if err := runServer(context.Background(), pipe); err != nil {
			log.Fatalf("error: %s", err)
		}
	case "client":
		if flag.NArg() != 4 {
			usage()
		}
		pipe := flag.Arg(1)
		iters, err := strconv.Atoi(flag.Arg(2))
		if err != nil {
			log.Fatalf("failed parsing iters: %s", err)
		}
		workers, err := strconv.Atoi(flag.Arg(3))
		if err != nil {
			log.Fatalf("failed parsing workers: %s", err)
		}
		start := time.Now()
		if err := runClient(context.Background(), pipe, iters, workers); err != nil {
			log.Fatalf("runtime error: %s", err)
		}
		log.Printf("elapsed time: %v", time.Since(start))
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n\tttrpcstress server <PIPE>\n\tttrpcstress client <PIPE> <ITERATIONS> <WORKERS>\n")
	os.Exit(1)
}

func runServer(ctx context.Context, pipe string) error {
	// 0 buffer sizes for pipe is important to help deadlock to occur.
	// It can still occur if there is buffering, but it takes more IO volume to hit it.
	l, err := winio.ListenPipe(pipe, &winio.PipeConfig{InputBufferSize: 0, OutputBufferSize: 0})
	if err != nil {
		return err
	}
	server, err := ttrpc.NewServer()
	if err != nil {
		return err
	}
	server.Register("MYSERVICE", map[string]ttrpc.Method{
		"MYMETHOD": func(ctx context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			req := &payload{}
			if err := unmarshal(req); err != nil {
				log.Fatalf("failed unmarshalling request: %s", err)
			}
			id := req.Value
			log.Printf("got request: %d", id)
			return &payload{Value: id}, nil
		},
	})
	if err := server.Serve(ctx, l); err != nil {
		return err
	}
	return nil
}

func runClient(ctx context.Context, pipe string, iters int, workers int) error {
	c, err := winio.DialPipe(pipe, nil)
	if err != nil {
		return err
	}
	client := ttrpc.NewClient(c)
	ch := make(chan int)
	var eg errgroup.Group
	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for {
				i, ok := <-ch
				if !ok {
					return nil
				}
				if err := send(ctx, client, uint32(i)); err != nil {
					return err
				}
			}
		})
	}
	for i := 0; i < iters; i++ {
		ch <- i
	}
	close(ch)
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func send(ctx context.Context, client *ttrpc.Client, id uint32) error {
	var (
		req  = &payload{Value: id}
		resp = &payload{}
	)
	log.Printf("sending request: %d", id)
	if err := client.Call(ctx, "MYSERVICE", "MYMETHOD", req, resp); err != nil {
		return err
	}
	ret := resp.Value
	log.Printf("got response: %d", ret)
	if ret != id {
		return fmt.Errorf("expected return value %d but got %d", id, ret)
	}
	return nil
}
