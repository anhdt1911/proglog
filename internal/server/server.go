package server

import (
	"context"
	log_v1 "github.com/travisjeffery/proglog/api/v1"
)

type Config struct {
	CommitLog CommitLog
}

var _ log_v1.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	log_v1.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *log_v1.ProduceRequest) (log_v1.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &log_v1.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *log_v1.ConsumeRequest) (log_v1.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &log_v1.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream