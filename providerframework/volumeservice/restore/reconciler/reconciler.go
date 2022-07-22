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
	"time"

	log "github.com/sirupsen/logrus"
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"

	kahuclient "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
)

// Reconciler runs a periodic loop to reconcile the current state of volume back and update
// volume restore state
type Reconciler interface {
	Run(stopCh <-chan struct{})
}

// NewReconciler returns a new instance of Reconciler that waits loopPeriod
// between successive executions.
func NewReconciler(
	loopPeriod time.Duration,
	logger log.FieldLogger,
	volumeRestoreClient kahuclient.VolumeRestoreContentInterface,
	volumeRestoreLister kahulister.VolumeRestoreContentLister,
	driverClient pb.VolumeBackupClient) Reconciler {
	return &reconciler{
		loopPeriod:          loopPeriod,
		logger:              logger,
		volumeRestoreClient: volumeRestoreClient,
		volumeRestoreLister: volumeRestoreLister,
		driverClient:        driverClient,
	}
}

type reconciler struct {
	loopPeriod          time.Duration
	logger              log.FieldLogger
	volumeRestoreClient kahuclient.VolumeRestoreContentInterface
	volumeRestoreLister kahulister.VolumeRestoreContentLister
	driverClient        pb.VolumeBackupClient
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
	// check and update volume restore state
	volumeRestoreList, err := rc.volumeRestoreLister.List(labels.Everything())
	if err != nil {
		rc.logger.Errorf("Unable to get volume restore list. %s", err)
		return
	}
	for _, volumeRestore := range volumeRestoreList {
		if !volumeRestore.DeletionTimestamp.IsZero() {
			rc.logger.Debugf("Volume restore % is getting deleted. Skipping reconciliation",
				volumeRestore.Name)
			continue
		} else if volumeRestore.Status.Phase == kahuapi.VolumeRestoreContentPhaseCompleted {
			rc.logger.Debugf("Volume restore % is completed. Skipping %s reconciliation",
				volumeRestore.Name)
			continue
		} else if volumeRestore.Status.Phase != kahuapi.VolumeRestoreContentPhaseInProgress {
			continue
		}

		restoreCurrentState := volumeRestore.Status.RestoreState
		restoreHandles := make([]string, 0)
		for _, state := range restoreCurrentState {
			restoreHandles = append(restoreHandles, state.VolumeHandle)
		}
		stats, err := rc.driverClient.GetRestoreStat(context.TODO(), &pb.GetRestoreStatRequest{
			RestoreVolumeHandle: restoreHandles,
			Parameters:          volumeRestore.Spec.Parameters,
		})
		if err != nil {
			rc.logger.Errorf("Unable to get volume restore state for %s. %s", volumeRestore.Name, err)
			continue
		}

		// updated status
		for _, stat := range stats.RestoreVolumeStat {
			for i, restoreState := range volumeRestore.Status.RestoreState {
				if stat.RestoreVolumeHandle == restoreState.VolumeHandle {
					volumeRestore.Status.RestoreState[i].Progress = stat.Progress
				}
			}
		}

		statUpdateCompleted := true
		for _, restoreState := range volumeRestore.Status.RestoreState {
			if restoreState.Progress < 100 {
				statUpdateCompleted = false
				break
			}
		}

		phase := volumeRestore.Status.Phase
		if statUpdateCompleted {
			phase = kahuapi.VolumeRestoreContentPhaseCompleted
		}
		_, err = rc.updateVolumeRestoreStatus(volumeRestore, kahuapi.VolumeRestoreContentStatus{
			Phase: phase,
		})
		if err != nil {
			rc.logger.Errorf("Unable to update volume restore states for %s. %s", volumeRestore.Name, err)
			continue
		}
	}
}

func (rc *reconciler) updateVolumeRestoreStatus(restore *kahuapi.VolumeRestoreContent,
	status kahuapi.VolumeRestoreContentStatus) (*kahuapi.VolumeRestoreContent, error) {
	rc.logger.Infof("Updating status: volume restore content %s", restore.Name)
	currentCopy := restore.DeepCopy()

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

	if status.RestoreState != nil {
		currentCopy.Status.RestoreState = status.RestoreState
	}

	return rc.volumeRestoreClient.UpdateStatus(context.TODO(),
		currentCopy,
		metav1.UpdateOptions{})
}
