package coordinator

import "context"

// Server exposes the coordinator's tracked producer locations to workers.
type Server struct {
	UnimplementedCoordinatorServer
	tracker LocationTracker
}

// NewServer creates a coordinator service backed by the given tracker.
func NewServer(tracker LocationTracker) *Server {
	return &Server{tracker: tracker}
}

func (s *Server) GetLocations(ctx context.Context, request *LocationRequest) (*LocationResponse, error) {
	locations := s.tracker.GetLocations(request.GetJobId(), int(request.GetStageId()))
	response := &LocationResponse{}
	for _, location := range locations {
		response.Locations = append(response.Locations, &ProducerLocation{TaskId: int64(location.TaskID), WorkerAddress: location.WorkerAddress})
	}
	return response, nil
}
