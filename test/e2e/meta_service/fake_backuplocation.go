// Copyright 2022 The SODA Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta_service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/soda-cdm/kahu/providers/lib/go"
)

type FakeNFSDriver struct {
	unixSocketPath string
	dumpYard       string
	server         *grpc.Server
}

func NewFakeNFSDriver(unixSocketPath string, dumpYard string) *FakeNFSDriver {
	return &FakeNFSDriver{
		unixSocketPath: unixSocketPath,
		dumpYard:       dumpYard,
	}
}

func (f *FakeNFSDriver) RunDriver() error {
	log.Info("Starting fake NFS Server ...")

	lis, err := net.Listen("unix", f.unixSocketPath)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	f.server = grpc.NewServer(opts...)
	pb.RegisterMetaBackupServer(f.server, f)

	return f.server.Serve(lis)
}

func (f *FakeNFSDriver) StopDriver() error {
	log.Info("Stopping fake NFS Server ...")
	f.server.Stop()
	return nil
}

func (f *FakeNFSDriver) Upload(upload pb.MetaBackup_UploadServer) error {
	log.Info("Upload Called .... ")

	uploadRequest, err := upload.Recv()
	if err != nil {
		return status.Errorf(codes.Unknown, "failed with error %s", err)
	}

	fileInfo := uploadRequest.GetInfo()
	if fileInfo == nil {
		return status.Errorf(codes.InvalidArgument, "first request is not file info")
	}

	fileName := fileInfo.GetFileIdentifier()
	// TODO: support file attributes

	// create buffer t o store data
	fileData := bytes.Buffer{}

	for {
		uploadRequest, err = upload.Recv()
		// If there are no more requests
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("uploadRequest received error %s", err)
			return status.Errorf(codes.Unknown, "error receiving request")
		}

		data := uploadRequest.GetChunkData()
		_, err = fileData.Write(data)
		if err != nil {
			log.Errorf("error writing request %s", err)
			return status.Errorf(codes.Unknown, "error writing request %s", err)
		}
	}

	file, err := os.Create(filepath.Join(f.dumpYard, fileName))
	if err != nil {
		log.Errorf("file create error %s", err)
		return status.Errorf(codes.Unknown, "file create error %s", err)
	}

	_, err = fileData.WriteTo(file)
	if err != nil {
		log.Errorf("uploadRequest received error %s", err)
		return status.Errorf(codes.Unknown, "error receiving request %s", err)
	}

	err = upload.SendAndClose(&pb.Empty{})
	if err != nil {
		return status.Errorf(codes.Unknown, "failed to close and flush file. %s", err)
	}

	return nil
}

func (f *FakeNFSDriver) ObjectExists(context.Context,
	*pb.ObjectExistsRequest) (*pb.ObjectExistsResponse, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (f *FakeNFSDriver) Download(*pb.DownloadRequest, pb.MetaBackup_DownloadServer) error {
	return fmt.Errorf("Not Implemented")
}

func (f *FakeNFSDriver) Delete(context.Context, *pb.DeleteRequest) (*pb.Empty, error) {
	return nil, fmt.Errorf("Not Implemented")
}
