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

	log "github.com/sirupsen/logrus"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
)

const (
	defaultProviderName    = "kahu-volume-backup-provider"
	defaultProviderVersion = "v1"
)

type service struct {
	ctx context.Context
}

var _ pb.VolumeBackupServer = &service{}
var _ pb.IdentityServer = &service{}

func NewVolumeBackupService(ctx context.Context) *service {
	return &service{
		ctx: ctx,
	}
}

// StartBackup create backup of the provided volumes
func (svc *service) StartBackup(ctx context.Context, req *pb.StartBackupRequest) (*pb.StartBackupResponse, error) {
	log.Infof("Start Backup called with Request %+v", req)
	return nil, nil
}

// DeleteBackup delete given backup
func (svc *service) DeleteBackup(ctx context.Context, req *pb.DeleteBackupRequest) (*pb.DeleteBackupResponse, error) {
	log.Infof("Delete Backup called with Request %+v", req)
	return nil, nil
}

// CancelBackup cancel given backup
func (svc *service) CancelBackup(context.Context, *pb.CancelBackupRequest) (*pb.CancelBackupResponse, error) {
	return nil, nil
}

// GetBackupStat get backup statistics
func (svc *service) GetBackupStat(_ context.Context, req *pb.GetBackupStatRequest) (*pb.GetBackupStatResponse, error) {
	log.Infof("GetBackupStat called with Request %+v", req)
	return nil, nil
}

// Create volume from backup (for restore)
func (svc *service) CreateVolumeFromBackup(context.Context,
	*pb.CreateVolumeFromBackupRequest) (*pb.CreateVolumeFromBackupResponse, error) {
	return nil, nil
}

// Cancel given restore
func (svc *service) CancelRestore(context.Context,
	*pb.CancelRestoreRequest) (*pb.CancelRestoreResponse, error) {
	return nil, nil
}

// Get restore statistics
func (svc *service) GetRestoreStat(context.Context,
	*pb.GetRestoreStatRequest) (*pb.GetRestoreStatResponse, error) {
	return nil, nil
}

// GetProviderInfo returns the basic information from provider side
func (svc *service) GetProviderInfo(ctx context.Context, GetProviderInfoRequest *pb.GetProviderInfoRequest) (*pb.GetProviderInfoResponse, error) {
	log.Info("GetProviderInfo Called .... ")
	response := &pb.GetProviderInfoResponse{
		Provider: defaultProviderName,
		Version:  defaultProviderVersion}

	return response, nil
}

// GetProviderCapabilities returns the capabilities supported by provider
func (svc *service) GetProviderCapabilities(ctx context.Context, GetProviderCapabilitiesRequest *pb.GetProviderCapabilitiesRequest) (*pb.GetProviderCapabilitiesResponse, error) {
	log.Info("GetProviderCapabilities Called .... ")
	return &pb.GetProviderCapabilitiesResponse{
		Capabilities: []*pb.ProviderCapability{
			{
				Type: &pb.ProviderCapability_Service_{
					Service: &pb.ProviderCapability_Service{
						Type: pb.ProviderCapability_Service_VOLUME_BACKUP_SERVICE,
					},
				},
			},
		},
	}, nil
}

// Probe checks the healthy/availability state of the provider
func (svc *service) Probe(ctx context.Context, probeRequest *pb.ProbeRequest) (*pb.ProbeResponse, error) {
	log.Info("Probe invoked")
	return &pb.ProbeResponse{}, nil
}
