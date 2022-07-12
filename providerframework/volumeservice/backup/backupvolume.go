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
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
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

	volumes := make([]*v1.PersistentVolume, 0)
	for _, vol := range backup.Spec.Volumes {
		volumes = append(volumes, vol.DeepCopy())
	}

	_, err := ctrl.backupProviderClient.StartBackup(context.Background(), &pb.StartBackupRequest{
		BackupName: backup.Name,
		Pv:         volumes,
	})
	if err != nil {
		ctrl.logger.Errorf("Unable to start backup. %s", err)
		return err
	}

	//getStart := func(backupResponse *pb.StartBackupResponse,
	//	vbcClient kahuclient.VolumeBackupContentInterface) error {
	//
	//}

	return nil
}
