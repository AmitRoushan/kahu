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

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/soda-cdm/kahu/utils"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"time"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	kahuclient "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/informers/externalversions"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	"github.com/soda-cdm/kahu/controllers"
	"github.com/soda-cdm/kahu/providerframework/volumeservice/reconciler"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
)

const (
	controllerName = "volume-content-backup"

	volumeBackupFinalizer = "kahu.io/volume-backup-protection"
)

type controller struct {
	logger             log.FieldLogger
	genericController  controllers.Controller
	volumeBackupClient kahuclient.VolumeBackupContentInterface
	volumeBackupLister kahulister.VolumeBackupContentLister
	eventRecorder      record.EventRecorder
	providerClient     pb.VolumeBackupClient
	reconciler         reconciler.Reconciler
}

func NewController(kahuClient versioned.Interface,
	informer externalversions.SharedInformerFactory,
	eventBroadcaster record.EventBroadcaster,
	backupProviderClient pb.VolumeBackupClient,
	stopChan <-chan struct{}) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	backupController := &controller{
		logger:             logger,
		volumeBackupClient: kahuClient.KahuV1beta1().VolumeBackupContents(),
		volumeBackupLister: informer.Kahu().V1beta1().VolumeBackupContents().Lister(),
		providerClient:     backupProviderClient,
	}

	// construct controller interface to process worker queue
	genericController, err := controllers.NewControllerBuilder(controllerName).
		SetLogger(logger).
		SetHandler(backupController.processQueue).
		Build()
	if err != nil {
		return nil, err
	}

	// register to informer to receive events and push events to worker queue
	informer.Kahu().
		V1beta1().
		VolumeBackupContents().
		Informer().
		AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc:    genericController.Enqueue,
				DeleteFunc: genericController.Enqueue,
			},
		)

	// initialize event recorder
	eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme,
		v1.EventSource{Component: controllerName})
	backupController.eventRecorder = eventRecorder

	// reference back
	backupController.genericController = genericController

	backupController.reconciler = reconciler.NewReconciler(
		1*time.Second,
		logger.WithField("source", "reconciler"),
		backupController.volumeBackupClient,
		backupController.volumeBackupLister,
		backupController.providerClient)

	go backupController.reconciler.Run(stopChan)
	return genericController, err
}

func (ctrl *controller) processQueue(key string) error {
	ctrl.logger.Infof("Processing volume backup request for %s", key)

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.logger.Errorf("splitting key into namespace and name, error %s", err)
		return err
	}

	volumeBackup, err := ctrl.volumeBackupLister.Get(name)
	if err == nil {
		volumeBackupClone := volumeBackup.DeepCopy()
		// delete scenario
		if volumeBackupClone.DeletionTimestamp != nil {
			return ctrl.processDeleteVolumeBackup(volumeBackupClone)
		}

		if isInitNeeded(volumeBackupClone) {
			volumeBackupClone, err = ctrl.backupInitialize(volumeBackupClone)
			if err != nil {
				ctrl.logger.Errorf("failed to initialize finalizer backup(%s)", volumeBackupClone.Name)
				return err
			}
		}
		// process create and sync
		return ctrl.processVolumeBackup(volumeBackupClone)
	}
	if !apierrors.IsNotFound(err) {
		// re enqueue for processing
		return fmt.Errorf("error getting backup %s from informer", name)
	}

	// if not in lister, delete event
	// TODO(Amit Roushan): check with finalizer for delete event
	return ctrl.processDeleteVolumeBackup(volumeBackup)
}

func (ctrl *controller) processDeleteVolumeBackup(backup *kahuapi.VolumeBackupContent) error {
	ctrl.logger.Infof("Processing volume backup delete request for %v", backup)

	backupIdentifiers := make([]*pb.BackupIdentifier, 0)
	for _, volumeState := range backup.Status.BackupState {
		backupIdentifier := new(pb.BackupIdentifier)
		backupIdentifier.BackupHandle = volumeState.BackupHandle
		backupIdentifier.PvName = volumeState.VolumeName
	}

	_, err := ctrl.providerClient.DeleteBackup(context.Background(), &pb.DeleteBackupRequest{
		BackupContentName: backup.Name,
		BackupInfo:        backupIdentifiers,
	})
	if err != nil {
		ctrl.logger.Errorf("Unable to delete backup. %s", err)
		return err
	}

	if utils.ContainsFinalizer(backup, volumeBackupFinalizer) {
		backupClone := backup.DeepCopy()
		utils.RemoveFinalizer(backupClone, volumeBackupFinalizer)
		_, err := ctrl.updateBackup(backup, backupClone)
		if err != nil {
			ctrl.logger.Errorf("removing finalizer failed for %s", backup.Name)
		}
		return err
	}

	return nil
}

func (ctrl *controller) processVolumeBackup(backup *kahuapi.VolumeBackupContent) error {
	logger := ctrl.logger.WithField("backup", backup.Name)

	if backup.DeletionTimestamp != nil {
		if err := ctrl.deleteVolumeBackup(backup); err != nil {
			ctrl.logger.Errorf("Unable to delete volume backup. %s", err)
			return err
		}
		return nil
	}

	switch backup.Status.Phase {
	case "", kahuapi.VolumeBackupContentPhaseInit:
		volumes := make([]*v1.PersistentVolume, 0)
		for _, vol := range backup.Spec.Volumes {
			volumes = append(volumes, vol.DeepCopy())
		}
		response, err := ctrl.providerClient.StartBackup(context.Background(), &pb.StartBackupRequest{
			BackupContentName: backup.Name,
			Pv:                volumes,
		})
		if err != nil {
			ctrl.logger.Errorf("Unable to start backup. %s", err)
			backup, err = ctrl.updateStatus(backup, kahuapi.VolumeBackupContentStatus{
				Phase:         kahuapi.VolumeBackupContentPhaseFailed,
				FailureReason: fmt.Sprintf("Unable to start backup"),
			})
			return err
		}

		backupState := make([]kahuapi.VolumeState, 0)
		for _, backupIdentifier := range response.GetBackupInfo() {
			backupState = append(backupState, kahuapi.VolumeState{
				VolumeName:   backupIdentifier.GetPvName(),
				BackupHandle: backupIdentifier.GetBackupHandle(),
			})
		}

		// update backup status
		backup, err = ctrl.updateStatus(backup, kahuapi.VolumeBackupContentStatus{
			Phase:       kahuapi.VolumeBackupContentPhaseInProgress,
			BackupState: backupState,
		})

	default:
		logger.Infof("Ignoring volume backup state. The state gets handled by reconciler")
	}

	return nil
}

func (ctrl *controller) deleteVolumeBackup(backup *kahuapi.VolumeBackupContent) error {
	ctrl.logger.Infof("Deleting volume backup content %s", backup.Name)
	return nil
}

func (ctrl *controller) updateStatus(backup *kahuapi.VolumeBackupContent,
	status kahuapi.VolumeBackupContentStatus) (*kahuapi.VolumeBackupContent, error) {
	ctrl.logger.Infof("Updating status: volume backup content %s", backup.Name)
	if backup.Status.Phase != "" &&
		status.Phase != backup.Status.Phase {
		backup.Status.Phase = status.Phase
	}

	if status.FailureReason != "" {
		backup.Status.FailureReason = status.FailureReason
	}

	if backup.Status.StartTimestamp == nil &&
		status.StartTimestamp != nil {
		backup.Status.StartTimestamp = status.StartTimestamp
	}

	if status.BackupState != nil {
		backup.Status.BackupState = status.BackupState
	}

	return ctrl.volumeBackupClient.UpdateStatus(context.TODO(),
		backup,
		metav1.UpdateOptions{})
}

func isInitNeeded(backup *kahuapi.VolumeBackupContent) bool {
	if !utils.ContainsFinalizer(backup, volumeBackupFinalizer) ||
		backup.Status.Phase == "" {
		return true
	}

	return false
}

func (ctrl *controller) backupInitialize(
	backup *kahuapi.VolumeBackupContent) (*kahuapi.VolumeBackupContent, error) {
	backupClone := backup.DeepCopy()
	if !utils.ContainsFinalizer(backup, volumeBackupFinalizer) {
		utils.SetFinalizer(backupClone, volumeBackupFinalizer)
	}
	if backupClone.Status.Phase == "" {
		backupClone.Status.Phase = kahuapi.VolumeBackupContentPhaseInit
	}
	if backup.Status.StartTimestamp == nil {
		time := metav1.Now()
		backupClone.Status.StartTimestamp = &time
	}
	return ctrl.updateBackup(backup, backupClone)
}

func (ctrl *controller) updateBackup(
	oldBackup,
	newBackup *kahuapi.VolumeBackupContent) (*kahuapi.VolumeBackupContent, error) {
	origBytes, err := json.Marshal(oldBackup)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original backup")
	}

	updatedBytes, err := json.Marshal(newBackup)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated backup")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for backup")
	}

	updatedBackup, err := ctrl.volumeBackupClient.Patch(context.TODO(),
		oldBackup.Name,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{}, "status")
	if err != nil {
		return nil, errors.Wrap(err, "error patching backup")
	}

	return updatedBackup, nil
}
