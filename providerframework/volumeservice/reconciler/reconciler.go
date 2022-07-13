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

package reconciler

import (
	"context"
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"

	kahuclient "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
)

// Reconciler runs a periodic loop to reconcile the current state of volume back and update
// volume backup state
type Reconciler interface {
	Run(stopCh <-chan struct{})
}

// NewReconciler returns a new instance of Reconciler that waits loopPeriod
// between successive executions.
func NewReconciler(
	loopPeriod time.Duration,
	logger log.FieldLogger,
	volumeBackupClient kahuclient.VolumeBackupContentInterface,
	volumeBackupLister kahulister.VolumeBackupContentLister,
	providerClient pb.VolumeBackupClient) Reconciler {
	return &reconciler{
		loopPeriod:         loopPeriod,
		logger:             logger,
		volumeBackupClient: volumeBackupClient,
		volumeBackupLister: volumeBackupLister,
		providerClient:     providerClient,
	}
}

type reconciler struct {
	loopPeriod         time.Duration
	logger             log.FieldLogger
	volumeBackupClient kahuclient.VolumeBackupContentInterface
	volumeBackupLister kahulister.VolumeBackupContentLister
	providerClient     pb.VolumeBackupClient
}

func (rc *reconciler) Run(stopCh <-chan struct{}) {
	wait.Until(rc.reconciliationLoopFunc(), rc.loopPeriod, stopCh)
}

func (rc *reconciler) reconciliationLoopFunc() func() {
	return func() {
		rc.reconcile()
	}
}

func (rc *reconciler) reconcile() {
	// check and update volume backup state

	volumeBackupList, err := rc.volumeBackupLister.List(labels.Everything())
	if err != nil {
		rc.logger.Errorf("Unable to get volume backup list. %s", err)
		return
	}
	for _, volumeBackup := range volumeBackupList {
		if volumeBackup.DeletionTimestamp != nil {
			rc.logger.Errorf("Volume backup % is getting deleted. Skipping reconciliation",
				volumeBackup.Name)
			continue
		} else if volumeBackup.Status.Phase == kahuapi.VolumeBackupContentPhaseCompleted {
			rc.logger.Errorf("Volume backup % is completed. Skipping %s reconciliation",
				volumeBackup.Name)
			continue
		}

		backupCurrentState := volumeBackup.Status.BackupState
		backupHandles := make([]string, 0)
		backupHandleMap := make(map[string]string, 0)
		for _, state := range backupCurrentState {
			backupHandles = append(backupHandles, state.BackupHandle)
			backupHandleMap[state.BackupHandle] = state.VolumeName
		}
		stat, err := rc.providerClient.GetBackupStat(context.TODO(), &pb.GetBackupStatRequest{
			BackupHandle: backupHandles,
		})
		if err != nil {
			rc.logger.Errorf("Unable to get volume backup state for %s. %s", volumeBackup.Name, err)
			continue
		}
		updatedBackupState := make([]kahuapi.VolumeState, 0)
		// map all stat
		statMap := make(map[string]*pb.BackupStat)
		for _, backupStat := range stat.GetBackupStats() {
			statMap[backupStat.GetBackupHandle()] = backupStat
		}

		statCount := 0
		statUpdatecompleted := true
		for _, backupState := range backupCurrentState {
			stat, ok := statMap[backupState.BackupHandle]
			if !ok {
				rc.logger.Warningf("Unable to get volume stat from request handle. %s",
					backupState.BackupHandle)
				continue
			}
			volName, ok := backupHandleMap[backupState.BackupHandle]
			if !ok {
				rc.logger.Warningf("Unable to get backup handle info. %s",
					backupState.BackupHandle)
				continue
			}
			statCount += 1
			if stat.TotalBytes > stat.BytesDone {
				statUpdatecompleted = false
			}
			updatedBackupState = append(updatedBackupState, kahuapi.VolumeState{
				VolumeName:   volName,
				BackupHandle: backupState.BackupHandle,
				TotalBytes:   stat.TotalBytes,
				BytesDone:    stat.BytesDone,
			})
		}

		phase := volumeBackup.Status.Phase
		if len(backupCurrentState) == statCount && statUpdatecompleted {
			phase = kahuapi.VolumeBackupContentPhaseCompleted
		}
		_, err = rc.updateVolumeBackupStatus(volumeBackup, kahuapi.VolumeBackupContentStatus{
			BackupState: updatedBackupState,
			Phase:       phase,
		})
		if err != nil {
			rc.logger.Errorf("Unable to update volume backup states for %s. %s", volumeBackup.Name, err)
			continue
		}
	}
}

func (rc *reconciler) updateVolumeBackupStatus(backup *kahuapi.VolumeBackupContent,
	status kahuapi.VolumeBackupContentStatus) (*kahuapi.VolumeBackupContent, error) {
	rc.logger.Infof("Updating status: volume backup content %s", backup.Name)
	currentCopy, err := rc.volumeBackupLister.Get(backup.Name)
	if err != nil {
		rc.logger.Errorf("Unable to update status. %s", err)
		return nil, err
	}

	if currentCopy.Status.Phase != "" &&
		status.Phase != currentCopy.Status.Phase {
		currentCopy.Status.Phase = status.Phase
	}

	if status.FailureReason != "" {
		currentCopy.Status.FailureReason = status.FailureReason
	}

	if currentCopy.Status.StartTimestamp == nil &&
		status.StartTimestamp != nil {
		currentCopy.Status.StartTimestamp = status.StartTimestamp
	}

	if status.BackupState != nil {
		currentCopy.Status.BackupState = status.BackupState
	}

	return rc.volumeBackupClient.UpdateStatus(context.TODO(),
		currentCopy,
		metav1.UpdateOptions{})
}
