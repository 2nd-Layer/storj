// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package console

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"storj.io/common/sync2"
	"storj.io/storj/satellite/mailservice"
)

// Chore checks whether any emails need to be re-sent.
//
// architecture: Chore
type Chore struct {
	log  *zap.Logger
	Loop *sync2.Cycle

	service     *Service
	mailsender  *mailservice.Sender
	mailService *mailservice.Service
	config      Config
}

// NewChore instantiates Chore.
func NewChore(log *zap.Logger, service *Service, config Config) *Chore {
	return &Chore{
		log:  log,
		Loop: sync2.NewCycle(time.Nanosecond),

		service: service,
		config:  config,
	}
}

// Run starts the chore.
func (chore *Chore) Run(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)
	return chore.Loop.Run(ctx, func(ctx context.Context) (err error) {
		defer mon.Task()(&ctx)(&err)

		users, err := chore.service.GetUnverifiedNeedingReminder(ctx)
		fmt.Println("USERS:", users)
		for _, u := range users {
			fmt.Println("USERS:", u.Email)
			// chore.mailService.SendRenderedAsync()
		}
		return err
	})
}

// Close closes chore.
func (chore *Chore) Close() error {
	chore.Loop.Close()
	return nil
}
