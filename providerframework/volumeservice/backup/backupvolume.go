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
	"fmt"
	log "github.com/sirupsen/logrus"
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	kahuclient "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/informers/externalversions"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	"github.com/soda-cdm/kahu/controllers"
	pb "github.com/soda-cdm/kahu/providers/lib/go"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"sync"
	"time"
)

const (
	controllerName = "volume-content-backup"
)

type controller struct {
	logger               log.FieldLogger
	genericController    controllers.Controller
	volumeBackupClient   kahuclient.VolumeBackupContentInterface
	volumeBackupLister   kahulister.VolumeBackupContentLister
	eventRecorder        record.EventRecorder
	backupProviderClient pb.VolumeBackupClient
}

func NewController(kahuClient versioned.Interface,
	informer externalversions.SharedInformerFactory,
	eventBroadcaster record.EventBroadcaster,
	backupProviderClient pb.VolumeBackupClient) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	backupController := &controller{
		logger:               logger,
		volumeBackupClient:   kahuClient.KahuV1beta1().VolumeBackupContents(),
		volumeBackupLister:   informer.Kahu().V1beta1().VolumeBackupContents().Lister(),
		backupProviderClient: backupProviderClient,
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
		// delete scenario
		if volumeBackup.DeletionTimestamp != nil {
			return ctrl.processDeleteVolumeBackup(volumeBackup)
		}
		// process create and sync
		return ctrl.processVolumeBackup(volumeBackup)
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
	return nil
}

func (ctrl *controller) processVolumeBackup(backup *kahuapi.VolumeBackupContent) error {
	// setting up backup logger
	logger := ctrl.logger.WithField("backup", backup.Name)
	logger.Info("Validating volume backup")

	if backup.DeletionTimestamp != nil {
		if err := ctrl.deleteVolumeBackup(backup); err != nil {
			ctrl.logger.Errorf("Unable to delete volume backup. %s", err)
			return err
		}
		return nil
	}

	getVolumeBackupStat := func(ctx context.Context, backupResponse *pb.StartBackupResponse,
		backupClient pb.VolumeBackupClient) error {
		backupClient.GetBackupStat(ctx, &pb.GetBackupStatRequest{
			BackupName:
		})
	}

	switch backup.Status.Phase {
	case "", kahuapi.VolumeBackupContentPhaseInit:
		volumes := make([]*v1.PersistentVolume, 0)
		for _, vol := range backup.Spec.Volumes {
			volumes = append(volumes, vol.DeepCopy())
		}
		response, err := ctrl.backupProviderClient.StartBackup(context.Background(), &pb.StartBackupRequest{
			BackupName: backup.Name,
			Pv:         volumes,
		})
		if err != nil {
			ctrl.logger.Errorf("Unable to start backup. %s", err)
			backup, err = ctrl.updateStatus(backup, kahuapi.VolumeBackupContentStatus{
				Phase: kahuapi.VolumeBackupContentPhaseFailed,
				FailureReason: fmt.Sprintf("Unable to start backup"),
			})
			return err
		}

		backup, err = ctrl.updateStatus(backup, kahuapi.VolumeBackupContentStatus{
			Phase: kahuapi.VolumeBackupContentPhaseInProgress,
		})

		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())
		defer func() {
			ctrl.logger.Info("Waiting for backup to finish")

			// shut down worker queue
			ctrl.queue.ShutDown()

			// wait for workers to shut down
			wg.Wait()
			ctrl.logger.Info("All workers have finished")
		}()

		go func() {
			wait.Until(func() {
				if err := getVolumeBackupStat(ctx, response, ctrl.volumeBackupClient); err != nil {
					ctrl.logger.Errorf("failed to get backup stat")
					return
				}

				<-ctx.Done()
			}, time.Second, ctx.Done())
			wg.Done()
		}()

		select {

		}

	case kahuapi.VolumeBackupContentPhaseInProgress:

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
	currentCopy, err := ctrl.volumeBackupLister.Get(backup.Name)
	if err != nil {
		ctrl.logger.Errorf("Unable to update status. %s", err)
		return nil, err
	}

	if currentCopy.Status.Phase != "" &&
		status.Phase != currentCopy.Status.Phase {
		currentCopy.Status.Phase = status.Phase
	}

	if status.FailureReason != ""  {
		currentCopy.Status.FailureReason = status.FailureReason
	}

	if currentCopy.Status.StartTimestamp == nil &&
		status.StartTimestamp != nil  {
		currentCopy.Status.StartTimestamp = status.StartTimestamp
	}

	return  ctrl.volumeBackupClient.UpdateStatus(context.TODO(),
		currentCopy,
		metav1.UpdateOptions{})
}
