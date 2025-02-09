// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"fmt"

	"github.com/zeebo/clingy"
	"github.com/zeebo/errs"

	"storj.io/storj/cmd/uplinkng/ulext"
)

type cmdAccessRemove struct {
	ex ulext.External

	access string
}

func newCmdAccessRemove(ex ulext.External) *cmdAccessRemove {
	return &cmdAccessRemove{ex: ex}
}

func (c *cmdAccessRemove) Setup(params clingy.Parameters) {
	c.access = params.Arg("name", "Access name to delete").(string)
}

func (c *cmdAccessRemove) Execute(ctx clingy.Context) error {
	defaultName, accesses, err := c.ex.GetAccessInfo(true)
	if err != nil {
		return err
	}

	if c.access == defaultName {
		return errs.New("cannot delete current access")
	}
	if _, ok := accesses[c.access]; !ok {
		return errs.New("unknown access: %q", c.access)
	}

	delete(accesses, c.access)
	if err := c.ex.SaveAccessInfo(defaultName, accesses); err != nil {
		return err
	}

	fmt.Fprintf(ctx, "Removed access %q from %q\n", c.access, c.ex.AccessInfoFile())

	return nil
}
