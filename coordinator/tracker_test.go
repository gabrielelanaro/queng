package coordinator_test

import (
	"context"
	"net"
	"reflect"
	"sort"
	"testing"

	"github.com/gabrielelanaro/queng/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func startCoordinator(t *testing.T, tracker *coordinator.MemoryLocationTracker) coordinator.CoordinatorClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	coordinator.RegisterCoordinatorServer(server, coordinator.NewServer(tracker))
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///coordinator", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return coordinator.NewCoordinatorClient(conn)
}

func TestGetLocationsReturnsProducerTasksForStage(t *testing.T) {
	tracker := coordinator.NewMemoryLocationTracker()
	tracker.Track("job-1", 3, coordinator.OutputLocation{TaskID: 7, WorkerAddress: "worker-a:9000"})
	tracker.Track("job-1", 3, coordinator.OutputLocation{TaskID: 8, WorkerAddress: "worker-b:9000"})
	tracker.Track("job-1", 4, coordinator.OutputLocation{TaskID: 9, WorkerAddress: "other-stage:9000"})
	tracker.Track("job-2", 3, coordinator.OutputLocation{TaskID: 10, WorkerAddress: "other-job:9000"})
	client := startCoordinator(t, tracker)

	response, err := client.GetLocations(context.Background(), &coordinator.LocationRequest{JobId: "job-1", StageId: 3})
	if err != nil {
		t.Fatal(err)
	}
	var got []coordinator.OutputLocation
	for _, location := range response.Locations {
		got = append(got, coordinator.OutputLocation{TaskID: int(location.TaskId), WorkerAddress: location.WorkerAddress})
	}
	sort.Slice(got, func(i, j int) bool { return got[i].TaskID < got[j].TaskID })
	want := []coordinator.OutputLocation{{TaskID: 7, WorkerAddress: "worker-a:9000"}, {TaskID: 8, WorkerAddress: "worker-b:9000"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("locations = %v; want %v", got, want)
	}
}
