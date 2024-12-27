package main

import (
	"log/slog"
	"os"

	"github.com/daocloud/crproxy/cmd/crproxy/cluster"
	csync "github.com/daocloud/crproxy/cmd/crproxy/sync"
	"github.com/spf13/cobra"

	_ "github.com/daocloud/crproxy/storage/driver/obs"
	_ "github.com/daocloud/crproxy/storage/driver/oss"
	_ "github.com/docker/distribution/registry/storage/driver/s3-aws"
)

func init() {
	cmd.AddCommand(csync.NewCommand())
	cmd.AddCommand(cluster.NewCommand())
}

var (
	cmd = &cobra.Command{
		Use:   "crproxy",
		Short: "crproxy",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	pflag = cmd.Flags()
)

func main() {
	err := cmd.Execute()
	if err != nil {
		slog.Error("execute failed", "error", err)
		os.Exit(1)
	}
}
