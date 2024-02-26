package client

import (
	"context"
	"time"

	"github.com/denizgursoy/clerk_grpc/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/status"
)

type (
	MemberConfig struct {
		KeepAliveDuration time.Duration
	}
	ClerkServerConfig struct {
		Address string
	}

	Member struct {
		group         string
		id            string
		lastPartition Partition
		cancelFunc    context.CancelFunc
		config        MemberConfig
		grpcClient    proto.MemberServiceClient
		notifyChannel chan Partition
		pingTicker    *time.Ticker
	}

	Partition struct {
		Ordinal int
		Total   int
	}
)

var defaultMemberConfig = MemberConfig{
	KeepAliveDuration: 6 * time.Second,
}

func newMember(grpcClient proto.MemberServiceClient, member *proto.Member, c MemberConfig) *Member {
	return &Member{
		group:      member.Group,
		id:         member.Id,
		grpcClient: grpcClient,
		config:     c,
	}
}

// Start function initializes the pinging.
// It is a blocking function
func (m *Member) Start(c context.Context) <-chan Partition {
	ctx, cancelFunc := context.WithCancel(c)
	m.cancelFunc = cancelFunc
	m.notifyChannel = make(chan Partition)
	m.pingTicker = time.NewTicker(m.config.KeepAliveDuration)
	go m.statPinging(ctx)

	return m.notifyChannel
}

// statPinging ping the server until context is cancelled
func (m *Member) statPinging(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.pingTicker.C:
			partition, err := m.grpcClient.Ping(ctx, toProto(m))
			if err != nil {
				if errStatus, ok := status.FromError(err); ok {
					// Check gRPC status code and message
					log.Printf("gRPC status code: %d, message: %s", errStatus.Code(), errStatus.Message())
				}
				continue
			}
			m.changePartition(partition)
		}
	}
}

func (m *Member) changePartition(partition *proto.Partition) {
	if int(partition.Ordinal) != m.lastPartition.Ordinal ||
		int(partition.Total) != m.lastPartition.Total {
		newPartition := Partition{
			Ordinal: int(partition.Ordinal),
			Total:   int(partition.Total),
		}
		m.lastPartition = newPartition
		m.notifyChannel <- newPartition
	}
}

func (m *Member) terminate() error {
	m.cancelFunc()
	close(m.notifyChannel)
	m.pingTicker.Stop()

	return nil
}

func (m *Member) Remove() error {
	_, err := m.grpcClient.RemoveMember(context.Background(), toProto(m))
	if err != nil {
		return err
	}
	if err = m.terminate(); err != nil {
		return err
	}

	return nil
}

func toProto(m *Member) *proto.Member {
	return &proto.Member{
		Group: m.group,
		Id:    m.id,
	}
}

func (m *Member) ID() string {
	return m.id
}

func (m *Member) Group() string {
	return m.group
}

func (m *Member) Partition() Partition {
	return m.lastPartition
}
