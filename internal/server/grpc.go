package server

import (
	pb "test/internal/.proto/gen"
	"test/internal/domain/services"

	"google.golang.org/grpc"
)

type serverApi struct {
	Service services.SignallingService
	pb.UnimplementedMessageServiceServer
}

func Register(gRPC *grpc.Server, ss services.SignallingService) {
	pb.RegisterMessageServiceServer(gRPC, &serverApi{Service: ss})
}
