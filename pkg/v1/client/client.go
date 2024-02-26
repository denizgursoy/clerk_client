package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/denizgursoy/clerk_grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClerkClient struct {
	config     ClerkServerConfig
	grpcClient proto.MemberServiceClient
}

func NewClerkClient(config ClerkServerConfig) (*ClerkClient, error) {
	conn, err := grpc.Dial(config.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("could not start grpcClient: %w", err)
	}
	c := &ClerkClient{config: config}
	c.grpcClient = proto.NewMemberServiceClient(conn)

	return c, nil
}

func (c *ClerkClient) CreateMember(ctx context.Context, group string, cfg *MemberConfig) (*Member, error) {
	if len(strings.TrimSpace(group)) == 0 {
		return nil, ErrEmptyGroup
	}

	memberWithID, err := c.grpcClient.AddMember(ctx, &proto.MemberRequest{Group: group})
	if err != nil {
		return nil, err
	}

	return newMember(c.grpcClient, memberWithID, getDefaultMemberConfigIfEmpty(cfg)), nil
}

func getDefaultMemberConfigIfEmpty(cfg *MemberConfig) MemberConfig {
	if cfg != nil {
		return *cfg
	}

	return defaultMemberConfig
}
