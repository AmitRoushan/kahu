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

package utils

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
	providerservice "github.com/soda-cdm/kahu/providers/lib/go"
)

const (
	probeInterval = 1 * time.Second
)

func GetConfig(kubeConfig string) (config *restclient.Config, err error) {
	if kubeConfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeConfig)
	}

	return restclient.InClusterConfig()
}

func SetupSignalHandler(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Infof("Received signal %s, shutting down", sig)
		cancel()
	}()
}

func GetDynamicClient(config *restclient.Config) (dynamic.Interface, error) {
	return dynamic.NewForConfig(config)
}
func GetK8sClient(config *restclient.Config) (*kubernetes.Clientset, error) {
	return kubernetes.NewForConfig(config)
}

func GetgrpcConn(address string, port uint) (*grpc.ClientConn, error) {
	return metaservice.NewLBDial(fmt.Sprintf("%s:%d", address, port), grpc.WithInsecure())
}

func GetMetaserviceClient(grpcConnection *grpc.ClientConn) metaservice.MetaServiceClient {
	return metaservice.NewMetaServiceClient(grpcConnection)
}

func GetMetaserviceBackupClient(address string, port uint) metaservice.MetaService_BackupClient {

	grpcconn, err := metaservice.NewLBDial(fmt.Sprintf("%s:%d", address, port), grpc.WithInsecure())
	if err != nil {
		log.Errorf("error getting grpc connection %s", err)
		return nil
	}
	metaClient := metaservice.NewMetaServiceClient(grpcconn)

	backupClient, err := metaClient.Backup(context.Background())
	if err != nil {
		log.Errorf("error getting backupclient %s", err)
		return nil
	}
	return backupClient
}

func GetGRPCConnection(endpoint string, dialOptions ...grpc.DialOption) (grpc.ClientConnInterface, error) {
	dialOptions = append(dialOptions,
		grpc.WithInsecure(),                   // unix domain connection.
		grpc.WithBackoffMaxDelay(time.Second), // Retry every second after failure.
		grpc.WithBlock(),                      // Block until connection succeeds.
		grpc.WithChainUnaryInterceptor(
			AddGRPCRequestID, // add gRPC request id
		),
	)

	unixPrefix := "unix://"
	if strings.HasPrefix(endpoint, "/") {
		// It looks like filesystem path.
		endpoint = unixPrefix + endpoint
	}

	if !strings.HasPrefix(endpoint, unixPrefix) {
		return nil, fmt.Errorf("invalid unix domain path [%s]",
			endpoint)
	}

	return grpc.Dial(endpoint, dialOptions...)
}

func AddGRPCRequestID(ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption) error {
	return invoker(ctx, method, req, reply, cc, opts...)
}

func Probe(conn grpc.ClientConnInterface, timeout time.Duration) error {
	for {
		log.Info("Probing driver for readiness")
		probe := func(conn grpc.ClientConnInterface, timeout time.Duration) (bool, error) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			rsp, err := providerservice.
				NewIdentityClient(conn).
				Probe(ctx, &providerservice.ProbeRequest{})

			if err != nil {
				return false, err
			}

			r := rsp.GetReady()
			if r == nil {
				return true, nil
			}
			return r.GetValue(), nil
		}

		ready, err := probe(conn, timeout)
		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				return fmt.Errorf("driver probe failed: %s", err)
			}
			if st.Code() != codes.DeadlineExceeded {
				return fmt.Errorf("driver probe failed: %s", err)
			}
			// Timeout -> driver is not ready. Fall through to sleep() below.
			log.Warning("driver probe timed out")
		}
		if ready {
			return nil
		}
		// sleep for retry again
		time.Sleep(probeInterval)
	}
}
