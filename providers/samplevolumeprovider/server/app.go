/*
Copyright 2022 The SODA Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"fmt"
	"net"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	pb "github.com/soda-cdm/kahu/providers/lib/go"
	"github.com/soda-cdm/kahu/providers/samplevolumeprovider/server/options"
	"github.com/soda-cdm/kahu/utils"
	logOptions "github.com/soda-cdm/kahu/utils/logoptions"
)

const (
	// Volume Service component name
	componentService = "sample-volume-provider"
)

// NewProviderCommand creates a *cobra.Command object with default parameters
func NewProviderCommand() *cobra.Command {
	cleanFlagSet := pflag.NewFlagSet(componentService, pflag.ContinueOnError)

	serviceFlags := options.NewServiceFlags()
	loggingOptions := logOptions.NewLogOptions()

	cmd := &cobra.Command{
		Use:  componentService,
		Long: `Sample volume backup driver`,
		// Disabled flag parsing from cobra framework
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			// initial flag parse, since we disable cobra's flag parsing
			if err := cleanFlagSet.Parse(args); err != nil {
				log.Error("Failed to parse provider service flag ", err)
				_ = cmd.Usage()
				os.Exit(1)
			}

			// check if there are non-flag arguments in the command line
			cmds := cleanFlagSet.Args()
			if len(cmds) > 0 {
				log.Error("Unknown command ", cmds[0])
				_ = cmd.Usage()
				os.Exit(1)
			}

			// short-circuit on help
			help, err := cleanFlagSet.GetBool("help")
			if err != nil {
				log.Error(`"help" flag is non-bool`)
				os.Exit(1)
			}
			if help {
				_ = cmd.Help()
				return
			}

			// validate and apply initial service Flags
			if err := serviceFlags.Apply(); err != nil {
				log.Error("Failed to validate provider service flags ", err)
				os.Exit(1)
			}

			// validate and apply logging flags
			if err := loggingOptions.Apply(); err != nil {
				log.Error("Failed to apply logging flags ", err)
				os.Exit(1)
			}

			ctx, cancel := context.WithCancel(context.Background())
			// setup signal handler
			utils.SetupSignalHandler(cancel)

			// run the meta service
			if err := Run(ctx, *serviceFlags); err != nil {
				log.Error("Failed to run nfs provider service", err)
				os.Exit(1)
			}

		},
	}

	serviceFlags.AddFlags(cleanFlagSet)
	// add logging flags
	loggingOptions.AddFlags(cleanFlagSet)
	cleanFlagSet.BoolP("help", "h", false, fmt.Sprintf("help for %s", cmd.Name()))

	const usageFmt = "Usage:\n  %s\n\nFlags:\n%s"
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine(), cleanFlagSet.FlagUsagesWrapped(2))
		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine(),
			cleanFlagSet.FlagUsagesWrapped(2))
	})

	return cmd
}

func Run(ctx context.Context, serviceOptions options.ServiceFlags) error {
	log.Info("Starting Server ...")
	serviceOptions.Print()

	serverAddr, err := net.ResolveUnixAddr("unix", serviceOptions.UnixSocketPath)
	if err != nil {
		log.Fatal("failed to resolve unix addr")
	}

	lis, err := net.ListenUnix("unix", serverAddr)
	if err != nil {
		log.Fatal("failed to listen: ", err)
	}

	service := NewVolumeBackupService(ctx)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterVolumeBackupServer(grpcServer, service)
	pb.RegisterIdentityServer(grpcServer, service)

	// graceful close when context close
	go func(ctx context.Context, server *grpc.Server) {
		<-ctx.Done()
		server.Stop()
	}(ctx, grpcServer)

	return grpcServer.Serve(lis)
}
