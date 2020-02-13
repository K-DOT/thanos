package e2ethanos

import (
	"github.com/cortexproject/cortex/integration/e2e"
)

type Service struct {
	*e2e.HTTPService

	grpc int
}

func NewService(
	name string,
	image string,
	env map[string]string,
	command *e2e.Command,
	readiness *e2e.ReadinessProbe,
	http, grpc int,
	otherPorts ...int,
) *Service {
	return &Service{
		HTTPService: e2e.NewHTTPService(name, image, env, command, readiness, http, append(otherPorts, grpc)...),
		grpc:        grpc,
	}
}

func (s *Service) GRPCEndpoint() string { return s.Endpoint(s.grpc) }
