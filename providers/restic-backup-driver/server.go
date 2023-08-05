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

// Package server implements Restic provider service interfaces
package restic

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
	"github.com/soda-cdm/kahu/providers/restic-backup-driver/config"
)

type volBackupServer struct {
	ctx        context.Context
	config     config.Config
	kubeClient kubernetes.Interface
	logger     log.FieldLogger
}

// NewVolumeBackupServer creates a new volume backup service
func NewVolumeBackupServer(ctx context.Context,
	config config.Config) (pb.VolumeBackupServer, error) {
	return &volBackupServer{
		ctx:        ctx,
		config:     config,
		kubeClient: config.GetKubeClient(),
		logger:     log.WithField("module", "restic-driver"),
	}, nil
}

// NewIdentityServer creates a new Identify service
func NewIdentityServer(ctx context.Context,
	config config.Config) pb.IdentityServer {
	return &volBackupServer{
		ctx:    ctx,
		config: config,
	}
}

// GetProviderInfo returns the basic information from provider side
func (server *volBackupServer) GetProviderInfo(
	ctx context.Context,
	GetProviderInfoRequest *pb.GetProviderInfoRequest) (*pb.GetProviderInfoResponse, error) {
	log.Info("GetProviderInfo Called .... ")
	response := &pb.GetProviderInfoResponse{
		Provider: server.config.GetProviderName(),
		Version:  server.config.GetProviderVersion()}

	return response, nil
}

// GetProviderCapabilities returns the capabilities supported by provider
func (server *volBackupServer) GetProviderCapabilities(
	_ context.Context,
	_ *pb.GetProviderCapabilitiesRequest) (*pb.GetProviderCapabilitiesResponse,
	error) {
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
func (server *volBackupServer) Probe(ctx context.Context, probeRequest *pb.ProbeRequest) (*pb.ProbeResponse, error) {
	return &pb.ProbeResponse{}, nil
}

// Create backup of the provided volumes
func (server *volBackupServer) StartBackup(req *pb.StartBackupRequest,
	res pb.VolumeBackup_StartBackupServer) error {
	backupInfos := req.GetBackupInfo()

	if len(backupInfos) != 1 {
		return status.Error(codes.InvalidArgument, "Expect only one volume for backup")
	}

	var backupInfo *pb.VolBackup
	for _, info := range backupInfos {
		backupInfo = info // extract backup info
	}

	backupVolumePath := backupInfo.MountPath
	if backupVolumePath == "" {
		return status.Error(codes.InvalidArgument, "Backup volume path for restic driver expected")
	}

	if req.Location == "" {
		return status.Error(codes.InvalidArgument, "Backup repository path for restic driver expected")
	}

	repoIdentifier, err := remoteRepoIdentifier(req.Location, backupInfo.Pv.Name)
	if err != nil {
		return status.Error(codes.InvalidArgument, "Expect backup volume path for restic driver")
	}

	// backup tags
	tags := tags(backupInfo.Pv)
	// password file path
	passwdFile, err := passwdPath()
	if err != nil {
		return status.Error(codes.Internal,
			fmt.Sprintf("Failed to create password file. %s", err))
	}

	err = server.ensureRepoInit(repoIdentifier, passwdFile)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "failed to initialize repository with error: %s", err)
	}

	backupCMD := backupCommand(repoIdentifier, passwdFile, backupVolumePath, tags)
	summary, stderrBuf, err := runBackup(backupCMD, server.logger, server.backupStatusUpdaterFunc(backupInfo, res))
	if err != nil {
		if strings.Contains(stderrBuf, "snapshot is empty") {
			server.logger.Debugf("Restic backup got empty dir with %s path", backupVolumePath)
			return status.Error(codes.FailedPrecondition, "volume was empty so no snapshot was taken")
		}
		return status.Error(codes.FailedPrecondition,
			fmt.Sprintf("error running restic backup command %s with error: %v stderr: %v", backupCMD.String(), err, stderrBuf))
	}

	snapshotIDCmd := getSnapshotCommand(repoIdentifier, passwdFile, tags)
	snapshotID, err := getSnapshotID(snapshotIDCmd)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("error getting snapshot id with error: %v", err))
	}

	backupIdentifiers := make([]*pb.BackupIdentifier, 0)
	for _, backupInfo := range req.GetBackupInfo() {
		backupIdentifiers = append(backupIdentifiers, &pb.BackupIdentifier{
			PvName: backupInfo.Pv.Name,
			BackupIdentity: &pb.BackupIdentity{
				BackupHandle: snapshotID,
			},
		})
	}

	err = res.Send(&pb.StartBackupResponse{
		BackupInfo: backupIdentifiers,
	})
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("error sending snapshot id with error: %v", err))
	}

	server.logger.Infof("Run command=%s, stdout=%s, stderr=%s", backupCMD.String(), summary, stderrBuf)
	return nil
}

func (server *volBackupServer) backupStatusUpdaterFunc(backupInfo *pb.VolBackup,
	res pb.VolumeBackup_StartBackupServer) func(state kahuapi.VolumeBackupState) {
	return func(state kahuapi.VolumeBackupState) {
		err := res.Send(&pb.StartBackupResponse{
			BackupInfo: []*pb.BackupIdentifier{
				&pb.BackupIdentifier{
					PvName:   backupInfo.Pv.Name,
					Progress: state.Progress,
				},
			},
		})
		if err != nil {
			server.logger.Errorf("error sending snapshot id with error: %v", err)
		}
	}
}

func (server *volBackupServer) ensureRepoInit(repoIdentifier, passwordFile string) error {
	snapshotsCmd := snapshotsCommand(repoIdentifier, passwordFile)
	// use the '--latest=1' flag to minimize the amount of data fetched since
	// we're just validating that the repo exists and can be authenticated
	// to.
	// "--last" is replaced by "--latest=1" in restic v0.12.1
	snapshotsCmd.ExtraFlags = append(snapshotsCmd.ExtraFlags, "--latest=1")
	stdout, stderr, err := runCommand(snapshotsCmd.Cmd())
	if err != nil {
		err = errors.Wrapf(err, "error running command=%s, stdout=%s, stderr=%s", snapshotsCmd.Cmd().String(), stdout, stderr)
		if strings.Contains(err.Error(), "Is there a repository at the following location?") {
			return initRepository(repoIdentifier, passwordFile)
		}
		return err
	}

	return nil
}

// Delete given backup
func (server *volBackupServer) DeleteBackup(context.Context, *pb.DeleteBackupRequest) (*pb.DeleteBackupResponse, error) {
	return &pb.DeleteBackupResponse{}, nil
}

// Cancel given backup
func (server *volBackupServer) CancelBackup(ctx context.Context, req *pb.CancelBackupRequest) (*pb.CancelBackupResponse, error) {
	res, err := server.DeleteBackup(ctx, &pb.DeleteBackupRequest{
		BackupInfo: req.BackupInfo,
		Parameters: req.Parameters,
	})
	return &pb.CancelBackupResponse{
		Success: res.Success,
		Errors:  res.Errors,
	}, err
}

// Get backup statistics
func (server *volBackupServer) GetBackupStat(ctx context.Context,
	req *pb.GetBackupStatRequest) (*pb.GetBackupStatResponse, error) {
	stat := make([]*pb.BackupStat, 0)
	for _, info := range req.BackupInfo {
		stat = append(stat, &pb.BackupStat{
			BackupHandle: info.BackupHandle,
			Progress:     100,
		})
	}
	return &pb.GetBackupStatResponse{
		BackupStats: stat,
	}, nil
}

// Create volume from backup (for restore)
func (server *volBackupServer) CreateVolumeFromBackup(ctx context.Context,
	restoreReq *pb.CreateVolumeFromBackupRequest) (*pb.CreateVolumeFromBackupResponse, error) {
	return &pb.CreateVolumeFromBackupResponse{
		// VolumeIdentifiers: restoreIDs,
	}, nil
}

// Cancel given restore
func (server *volBackupServer) CancelRestore(context.Context, *pb.CancelRestoreRequest) (*pb.CancelRestoreResponse, error) {
	return &pb.CancelRestoreResponse{}, nil
}

// Get restore statistics
func (server *volBackupServer) GetRestoreStat(ctx context.Context, req *pb.GetRestoreStatRequest) (*pb.GetRestoreStatResponse, error) {
	restoreStats := make([]*pb.RestoreStat, 0)
	return &pb.GetRestoreStatResponse{
		RestoreVolumeStat: restoreStats,
	}, nil
}
