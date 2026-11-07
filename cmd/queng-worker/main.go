// queng-worker runs a worker gRPC service.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"time"

	"github.com/gabrielelanaro/queng/coordinator"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/remoteexchange"
	"github.com/gabrielelanaro/queng/worker"
	"github.com/gabrielelanaro/queng/workerapi"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.000"})
	listen := flag.String("listen", ":9001", "worker gRPC listen address")
	coordinatorAddress := flag.String("coordinator", "localhost:9000", "coordinator gRPC address")
	flag.Parse()

	options := grpc.WithTransportCredentials(insecure.NewCredentials())
	coordinatorConn, err := grpc.NewClient(*coordinatorAddress, options)
	if err != nil {
		log.Fatal(err)
	}
	defer coordinatorConn.Close()
	clients := worker.NewClients(options)
	defer clients.Close()
	exchange := remoteexchange.New(distributed.NewMemoryExchange(), coordinator.NewCoordinatorClient(coordinatorConn), clients)
	defer exchange.Close()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	workerapi.RegisterWorkerServer(server, &worker.Server{Exchange: exchange})
	log.Printf("worker listening on %s", *listen)
	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
