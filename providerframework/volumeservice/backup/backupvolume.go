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
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	kahuscheme "github.com/soda-cdm/kahu/client/clientset/versioned/scheme"
	kahuclient "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/informers/externalversions"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	"github.com/soda-cdm/kahu/controllers"
	"github.com/soda-cdm/kahu/providerframework/volumeservice/backup/reconciler"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
	"github.com/soda-cdm/kahu/utils"
)

const (
	controllerName = "volume-backup-content"

	volumeBackupFinalizer = "kahu.io/volume-backup-protection"
)

type controller struct {
	logger               log.FieldLogger
	genericController    controllers.Controller
	volumeBackupClient   kahuclient.VolumeBackupContentInterface
	volumeBackupLister   kahulister.VolumeBackupContentLister
	backupLocationLister kahulister.BackupLocationLister
	providerLister       kahulister.ProviderLister
	eventRecorder        record.EventRecorder
	providerClient       pb.VolumeBackupClient
	reconciler           reconciler.Reconciler
}

func NewController(kahuClient versioned.Interface,
	informer externalversions.SharedInformerFactory,
	eventBroadcaster record.EventBroadcaster,
	backupProviderClient pb.VolumeBackupClient,
	stopChan <-chan struct{}) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	backupController := &controller{
		logger:               logger,
		volumeBackupClient:   kahuClient.KahuV1beta1().VolumeBackupContents(),
		volumeBackupLister:   informer.Kahu().V1beta1().VolumeBackupContents().Lister(),
		backupLocationLister: informer.Kahu().V1beta1().BackupLocations().Lister(),
		providerLister:       informer.Kahu().V1beta1().Providers().Lister(),
		providerClient:       backupProviderClient,
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
				AddFunc: genericController.Enqueue,
				UpdateFunc: func(oldObj, newObj interface{}) {
					genericController.Enqueue(newObj)
				},
			},
		)

	// initialize event recorder
	eventRecorder := eventBroadcaster.NewRecorder(kahuscheme.Scheme,
		v1.EventSource{Component: controllerName})
	backupController.eventRecorder = eventRecorder

	// reference back
	backupController.genericController = genericController

	backupController.reconciler = reconciler.NewReconciler(
		5*time.Second,
		logger.WithField("source", "reconciler"),
		backupController.volumeBackupClient,
		backupController.volumeBackupLister,
		backupController.backupLocationLister,
		backupController.providerLister,
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
				ctrl.logger.Errorf("failed to initialize finalizer backup(%s)", key)
				return err
			}
		}
		// process create and sync
		return ctrl.processVolumeBackup(volumeBackupClone)
	}

	return err
}

func (ctrl *controller) processDeleteVolumeBackup(backup *kahuapi.VolumeBackupContent) error {
	ctrl.logger.Infof("Processing volume backup delete request for %v", backup)

	backupIdentifiers := make([]*pb.BackupIdentifier, 0)
	for _, volumeState := range backup.Status.BackupState {
		backupIdentifier := new(pb.BackupIdentifier)
		backupIdentifier.BackupHandle = volumeState.BackupHandle
		backupIdentifier.PvName = volumeState.VolumeName
		backupIdentifiers = append(backupIdentifiers, backupIdentifier)
	}

	_, err := ctrl.providerClient.DeleteBackup(context.Background(), &pb.DeleteBackupRequest{
		BackupContentName: backup.Name,
		BackupInfo:        backupIdentifiers,
	})
	if err != nil {
		ctrl.logger.Errorf("Unable to delete volume backup. %s", err)
		return err
	}

	ctrl.logger.Infof("Volume backup driver delete (%s) successfully", backup.Name)

	backupClone := backup.DeepCopy()
	utils.RemoveFinalizer(backupClone, volumeBackupFinalizer)
	_, err = ctrl.patchBackup(backup, backupClone)
	if err != nil {
		ctrl.logger.Errorf("removing finalizer failed for %s", backup.Name)
		return err
	}
	ctrl.logger.Infof("Volume backup (%s) delete successfully", backup.Name)
	return nil
}

func (ctrl *controller) processVolumeBackup(backup *kahuapi.VolumeBackupContent) error {
	logger := ctrl.logger.WithField("backup", backup.Name)

	switch backup.Status.Phase {
	case "", kahuapi.VolumeBackupContentPhaseInit:
		parameters, err := ctrl.getBackupParam(backup)
		if err != nil {
			backup, err = ctrl.updateStatus(backup, kahuapi.VolumeBackupContentStatus{
				Phase:         kahuapi.VolumeBackupContentPhaseFailed,
				FailureReason: fmt.Sprintf("Uanble to get backup parameters"),
			})
			return err
		}

		volumes := make([]*v1.PersistentVolume, 0)
		for _, vol := range backup.Spec.Volumes {
			volumes = append(volumes, vol.DeepCopy())
		}
		response, err := ctrl.providerClient.StartBackup(context.Background(), &pb.StartBackupRequest{
			BackupContentName: backup.Name,
			Pv:                volumes,
			Parameters:        parameters,
		})
		if err != nil {
			ctrl.logger.Errorf("Unable to start backup. %s", err)
			backup, err = ctrl.updateStatus(backup, kahuapi.VolumeBackupContentStatus{
				Phase:         kahuapi.VolumeBackupContentPhaseFailed,
				FailureReason: fmt.Sprintf("Unable to start backup"),
			})
			return err
		}

		backupState := make([]kahuapi.VolumeBackupState, 0)
		for _, backupIdentifier := range response.GetBackupInfo() {
			backupState = append(backupState, kahuapi.VolumeBackupState{
				VolumeName:   backupIdentifier.GetPvName(),
				BackupHandle: backupIdentifier.GetBackupHandle(),
			})
		}

		// update backup status
		backup, err = ctrl.updateStatus(backup, kahuapi.VolumeBackupContentStatus{
			Phase:         kahuapi.VolumeBackupContentPhaseInProgress,
			BackupState:   backupState,
			FailureReason: "",
		})
		if err != nil {
			ctrl.logger.Errorf("Volume backup failed %s", backup.Name)
			return err
		}
		ctrl.logger.Infof("Volume backup scheduled %s", backup.Name)
	default:
		logger.Infof("Ignoring volume backup state. The state gets handled by reconciler")
	}

	return nil
}

func isInitNeeded(backup *kahuapi.VolumeBackupContent) bool {
	if backup.Status.Phase == "" ||
		backup.Status.Phase == kahuapi.VolumeBackupContentPhaseInit {
		return true
	}

	return false
}

func (ctrl *controller) backupInitialize(
	backup *kahuapi.VolumeBackupContent) (*kahuapi.VolumeBackupContent, error) {
	backupClone := backup.DeepCopy()

	utils.SetFinalizer(backupClone, volumeBackupFinalizer)

	if backupClone.Status.Phase == "" {
		backupClone.Status.Phase = kahuapi.VolumeBackupContentPhaseInit
	}
	if backup.Status.StartTimestamp == nil {
		time := metav1.Now()
		backupClone.Status.StartTimestamp = &time
	}
	return ctrl.patchBackup(backup, backupClone)
}

func (ctrl *controller) patchBackup(
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
		metav1.PatchOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error patching backup")
	}

	return updatedBackup, nil
}

func (ctrl *controller) updateStatus(
	backup *kahuapi.VolumeBackupContent,
	status kahuapi.VolumeBackupContentStatus) (*kahuapi.VolumeBackupContent, error) {
	ctrl.logger.Infof("Updating status: volume backup content %s", backup.Name)
	if status.Phase != "" &&
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

func (ctrl *controller) getBackupParam(
	backup *kahuapi.VolumeBackupContent) (map[string]string, error) {
	parameters := make(map[string]string)

	backupLocation, err := utils.GetBackupLocation(
		ctrl.logger,
		backup.Spec.BackupLocationName,
		ctrl.backupLocationLister)
	if err != nil {
		ctrl.logger.Errorf("Unable to get backup location info. %s", err)
		return parameters, errors.Wrap(err, "unable to get backup location parameters")
	}

	provider, err := utils.GetProvider(ctrl.logger,
		backupLocation.Spec.ProviderName,
		ctrl.providerLister)
	if err != nil {
		ctrl.logger.Errorf("Unable to get provider parameters. %s", err)
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
