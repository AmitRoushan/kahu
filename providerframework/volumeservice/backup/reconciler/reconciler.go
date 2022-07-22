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
	"fmt"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"

	kahuclient "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
	"github.com/soda-cdm/kahu/utils"
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
	backupLocationLister kahulister.BackupLocationLister,
	providerLister kahulister.ProviderLister,
	driverClient pb.VolumeBackupClient) Reconciler {
	return &reconciler{
		loopPeriod:           loopPeriod,
		logger:               logger,
		volumeBackupClient:   volumeBackupClient,
		volumeBackupLister:   volumeBackupLister,
		backupLocationLister: backupLocationLister,
		providerLister:       providerLister,
		driver:               driverClient,
	}
}

type reconciler struct {
	loopPeriod           time.Duration
	logger               log.FieldLogger
	volumeBackupClient   kahuclient.VolumeBackupContentInterface
	volumeBackupLister   kahulister.VolumeBackupContentLister
	backupLocationLister kahulister.BackupLocationLister
	providerLister       kahulister.ProviderLister
	driver               pb.VolumeBackupClient
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
			rc.logger.Debugf("Volume backup % is getting deleted. Skipping reconciliation",
				volumeBackup.Name)
			continue
		} else if volumeBackup.Status.Phase == kahuapi.VolumeBackupContentPhaseCompleted {
			rc.logger.Debugf("Volume backup % is completed. Skipping %s reconciliation",
				volumeBackup.Name)
			continue
		} else if volumeBackup.Status.Phase != kahuapi.VolumeBackupContentPhaseInProgress {
			continue
		}

		parameters, err := rc.getBackupParam(volumeBackup)
		if err != nil {
			_, err = rc.updateVolumeBackupStatus(volumeBackup, kahuapi.VolumeBackupContentStatus{
				Phase:         kahuapi.VolumeBackupContentPhaseFailed,
				FailureReason: fmt.Sprintf("Uanble to get backup parameters"),
			})
			continue
		}

		backupCurrentState := volumeBackup.Status.BackupState
		backupHandles := make([]string, 0)
		backupHandleMap := make(map[string]string, 0)
		for _, state := range backupCurrentState {
			backupHandles = append(backupHandles, state.BackupHandle)
			backupHandleMap[state.BackupHandle] = state.VolumeName
		}
		stat, err := rc.driver.GetBackupStat(context.TODO(), &pb.GetBackupStatRequest{
			BackupHandle: backupHandles,
			Parameters:   parameters,
		})
		if err != nil {
			rc.logger.Errorf("Unable to get volume backup state for %s. %s", volumeBackup.Name, err)
			continue
		}
		updatedBackupState := make([]kahuapi.VolumeBackupState, 0)
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
			if stat.Progress < 100 {
				statUpdatecompleted = false
			}
			updatedBackupState = append(updatedBackupState, kahuapi.VolumeBackupState{
				VolumeName:   volName,
				BackupHandle: backupState.BackupHandle,
				Progress:     backupState.Progress,
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
	currentCopy := backup.DeepCopy()

	if status.Phase != "" &&
		status.Phase != currentCopy.Status.Phase {
		currentCopy.Status.Phase = status.Phase
	}

	if status.FailureReason != "" {
		currentCopy.Status.FailureReason = status.FailureReason
	}

	if currentCopy.Status.StartTimestamp.IsZero() &&
		!status.StartTimestamp.IsZero() {
		currentCopy.Status.StartTimestamp = status.StartTimestamp
	}

	if status.BackupState != nil {
		currentCopy.Status.BackupState = status.BackupState
	}

	//rc.logger.Infof("status update %+v", currentCopy)
	return rc.volumeBackupClient.UpdateStatus(context.TODO(),
		currentCopy,
		metav1.UpdateOptions{})
}

func (rc *reconciler) getBackupParam(
	backup *kahuapi.VolumeBackupContent) (map[string]string, error) {
	parameters := make(map[string]string)

	backupLocation, err := utils.GetBackupLocation(
		rc.logger,
		backup.Spec.BackupLocationName,
		rc.backupLocationLister)
	if err != nil {
		rc.logger.Errorf("Unable to get backup location info. %s", err)
		return parameters, errors.Wrap(err, "unable to get backup location parameters")
	}

	provider, err := utils.GetProvider(rc.logger,
		backupLocation.Spec.ProviderName,
		rc.providerLister)
	if err != nil {
		rc.logger.Errorf("Unable to get provider parameters. %s", err)
		return parameters, errors.Wrap(err, "unable to get provider parameters")
	}

	for key, value := range provider.Spec.Manifest {
		parameters[key] = value
	}

	for key, value := range backupLocation.Spec.Config {
		parameters[key] = value
	}

	return parameters, nil
}
