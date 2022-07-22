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

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/soda-cdm/kahu/utils"
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
	"github.com/soda-cdm/kahu/providerframework/volumeservice/restore/reconciler"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
)

const (
	controllerName = "volume-restore-content"

	volumeRestoreFinalizer = "kahu.io/volume-restore-protection"
)

type controller struct {
	logger              log.FieldLogger
	genericController   controllers.Controller
	volumeRestoreClient kahuclient.VolumeRestoreContentInterface
	volumeRestoreLister kahulister.VolumeRestoreContentLister
	eventRecorder       record.EventRecorder
	driver              pb.VolumeBackupClient
	reconciler          reconciler.Reconciler
}

func NewController(
	kahuClient versioned.Interface,
	informer externalversions.SharedInformerFactory,
	eventBroadcaster record.EventBroadcaster,
	driver pb.VolumeBackupClient,
	stopChan <-chan struct{}) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	restoreController := &controller{
		logger:              logger,
		volumeRestoreClient: kahuClient.KahuV1beta1().VolumeRestoreContents(),
		volumeRestoreLister: informer.Kahu().V1beta1().VolumeRestoreContents().Lister(),
		driver:              driver,
	}

	// construct controller interface to process worker queue
	genericController, err := controllers.NewControllerBuilder(controllerName).
		SetLogger(logger).
		SetHandler(restoreController.processQueue).
		Build()
	if err != nil {
		return nil, err
	}

	// register to informer to receive events and push events to worker queue
	informer.Kahu().
		V1beta1().
		VolumeRestoreContents().
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
	restoreController.eventRecorder = eventRecorder

	// reference back
	restoreController.genericController = genericController

	restoreController.reconciler = reconciler.NewReconciler(
		5*time.Second,
		logger.WithField("source", "restore-reconciler"),
		restoreController.volumeRestoreClient,
		restoreController.volumeRestoreLister,
		restoreController.driver)

	go restoreController.reconciler.Run(stopChan)
	return genericController, err
}

func (ctrl *controller) processQueue(index string) error {
	ctrl.logger.Infof("Processing volume restore request for %s", index)

	_, name, err := cache.SplitMetaNamespaceKey(index)
	if err != nil {
		ctrl.logger.Errorf("splitting key into namespace and name, error %s", err)
		return err
	}

	volumeRestore, err := ctrl.volumeRestoreLister.Get(name)
	if err == nil {
		volumeRestoreClone := volumeRestore.DeepCopy()
		// delete scenario
		if volumeRestoreClone.DeletionTimestamp != nil {
			return ctrl.processDeleteVolumeRestore(volumeRestoreClone)
		}

		if isInitNeeded(volumeRestoreClone) {
			volumeRestoreClone, err = ctrl.restoreInitialize(volumeRestoreClone)
			if err != nil {
				ctrl.logger.Errorf("failed to initialize finalizer restore(%s)", index)
				return err
			}
		}
		// process create and sync
		return ctrl.processVolumeRestore(volumeRestoreClone)
	}

	return err
}

func (ctrl *controller) processDeleteVolumeRestore(restore *kahuapi.VolumeRestoreContent) error {
	ctrl.logger.Infof("Processing volume backup delete request for %v", restore)

	restoreClone := restore.DeepCopy()
	utils.RemoveFinalizer(restoreClone, volumeRestoreFinalizer)
	_, err := ctrl.patchBackup(restore, restoreClone)
	if err != nil {
		ctrl.logger.Errorf("removing finalizer failed for %s", restore.Name)
		return err
	}
	ctrl.logger.Infof("Volume restore (%s) delete successfully", restore.Name)
	return nil
}

func (ctrl *controller) processVolumeRestore(restore *kahuapi.VolumeRestoreContent) error {
	logger := ctrl.logger.WithField("restore", restore.Name)

	switch restore.Status.Phase {
	case "", kahuapi.VolumeRestoreContentPhaseInit:
		identifiers := make([]*pb.RestoreIdentifier, 0)
		for _, volume := range restore.Spec.Volumes {
			identifiers = append(identifiers, &pb.RestoreIdentifier{
				Pvc:          volume.Claim.DeepCopy(),
				BackupHandle: volume.BackupHandle,
			})
		}
		// TODO (Amit Roushan): can add retry logic here
		response, err := ctrl.driver.CreateVolumeFromBackup(context.Background(),
			&pb.CreateVolumeFromBackupRequest{
				RestoreInfo:        identifiers,
				RestoreContentName: restore.Name,
				Parameters:         restore.Spec.Parameters,
			})
		if err != nil {
			ctrl.logger.Errorf("Unable to start restore. %s", err)
			restore, err = ctrl.updateStatus(restore, kahuapi.VolumeRestoreContentStatus{
				Phase:         kahuapi.VolumeRestoreContentPhaseFailed,
				FailureReason: fmt.Sprintf("Unable to start restore"),
			})
			return err
		}
		if len(response.GetErrors()) > 0 {
			return fmt.Errorf("unable to create volume. %s",
				strings.Join(response.GetErrors(), ", "))
		}

		restoreState := make([]kahuapi.VolumeRestoreState, 0)
		for _, restoreIdentifier := range response.GetVolumeIdentifier() {
			restoreState = append(restoreState, kahuapi.VolumeRestoreState{
				VolumeName:   restoreIdentifier.PvcName,
				VolumeHandle: restoreIdentifier.VolumeHandle,
			})
		}

		// update restore status
		restore, err = ctrl.updateStatus(restore, kahuapi.VolumeRestoreContentStatus{
			Phase:        kahuapi.VolumeRestoreContentPhaseInProgress,
			RestoreState: restoreState,
		})
		if err != nil {
			ctrl.logger.Errorf("Volume restore failed %s", restore.Name)
			return err
		}
		ctrl.logger.Infof("Volume restore scheduled %s", restore.Name)
	default:
		logger.Infof("Ignoring volume restore state. The state gets handled by reconciler")
	}

	return nil
}

func isInitNeeded(restore *kahuapi.VolumeRestoreContent) bool {
	if restore.Status.Phase == "" ||
		restore.Status.Phase == kahuapi.VolumeRestoreContentPhaseInit {
		return true
	}

	return false
}

func (ctrl *controller) restoreInitialize(
	restore *kahuapi.VolumeRestoreContent) (*kahuapi.VolumeRestoreContent, error) {
	restoreClone := restore.DeepCopy()

	utils.SetFinalizer(restoreClone, volumeRestoreFinalizer)
	restoreClone, err := ctrl.patchBackup(restore, restoreClone)
	if err != nil {
		return restoreClone, err
	}

	status := kahuapi.VolumeRestoreContentStatus{}
	if restoreClone.Status.Phase == "" {
		status.Phase = kahuapi.VolumeRestoreContentPhaseInit
	}
	if restore.Status.StartTimestamp.IsZero() {
		time := metav1.Now()
		status.StartTimestamp = &time
	}
	return ctrl.updateStatus(restore, status)
}

func (ctrl *controller) patchBackup(
	oldRestore,
	newRestore *kahuapi.VolumeRestoreContent) (*kahuapi.VolumeRestoreContent, error) {
	origBytes, err := json.Marshal(oldRestore)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original restore")
	}

	updatedBytes, err := json.Marshal(newRestore)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated restore")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for restore")
	}

	updatedBackup, err := ctrl.volumeRestoreClient.Patch(context.TODO(),
		oldRestore.Name,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error patching restore")
	}

	return updatedBackup, nil
}

func (ctrl *controller) updateStatus(
	restore *kahuapi.VolumeRestoreContent,
	status kahuapi.VolumeRestoreContentStatus) (*kahuapi.VolumeRestoreContent, error) {
	ctrl.logger.Infof("Updating status: volume restore content %s", restore.Name)
	if status.Phase != "" &&
		status.Phase != restore.Status.Phase {
		restore.Status.Phase = status.Phase
	}

	if status.FailureReason != "" {
		restore.Status.FailureReason = status.FailureReason
	}

	if restore.Status.StartTimestamp.IsZero() &&
		!status.StartTimestamp.IsZero() {
		restore.Status.StartTimestamp = status.StartTimestamp
	}

	if status.RestoreState != nil {
		mergeStatusResources(restore, status)
	}

	return ctrl.volumeRestoreClient.UpdateStatus(context.TODO(),
		restore,
		metav1.UpdateOptions{})
}

func mergeStatusResources(restore *kahuapi.VolumeRestoreContent,
	status kahuapi.VolumeRestoreContentStatus) {
	newStates := make([]kahuapi.VolumeRestoreState, 0)
	for _, state := range status.RestoreState {
		found := false
		for _, restoreState := range restore.Status.RestoreState {
			if state.VolumeName == restoreState.VolumeName {
				found = true
				break
			}
		}
		if !found {
			newStates = append(newStates, state)
		}
	}
	restore.Status.RestoreState = append(restore.Status.RestoreState, newStates...)
}
