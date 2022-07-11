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
	"regexp"
	//"sort"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/soda-cdm/kahu/client/informers/externalversions"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	//"k8s.io/apimachinery/pkg/labels"
	//"k8s.io/apimachinery/pkg/runtime"
	//"k8s.io/apimachinery/pkg/runtime/schema"
	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	"github.com/soda-cdm/kahu/client/clientset/versioned/scheme"
	kahuv1client "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	"github.com/soda-cdm/kahu/controllers"
	//metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
	"github.com/soda-cdm/kahu/utils"
)

const (
	controllerName = "backup-controller"

	backupFinalizer = "kahu.io/backup-protection"
)

type controller struct {
	logger               log.FieldLogger
	genericController    controllers.Controller
	kubeClient           kubernetes.Interface
	dynamicClient        dynamic.Interface
	backupClient         kahuv1client.BackupInterface
	backupLister         kahulister.BackupLister
	backupLocationLister kahulister.BackupLocationLister
	eventRecorder        record.EventRecorder
	// backupCache          cache.Store
}

func NewController(
	kubeClient kubernetes.Interface,
	kahuClient versioned.Interface,
	dynamicClient dynamic.Interface,
	informer externalversions.SharedInformerFactory,
	eventBroadcaster record.EventBroadcaster) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	backupController := &controller{
		logger:               logger,
		kubeClient:           kubeClient,
		backupClient:         kahuClient.KahuV1beta1().Backups(),
		backupLister:         informer.Kahu().V1beta1().Backups().Lister(),
		backupLocationLister: informer.Kahu().V1beta1().BackupLocations().Lister(),
		dynamicClient:        dynamicClient,
		// backupCache:          cache.NewStore(cache.MetaNamespaceKeyFunc),
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
		Backups().
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
	eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme,
		v1.EventSource{Component: controllerName})
	backupController.eventRecorder = eventRecorder

	// reference back
	backupController.genericController = genericController
	return genericController, err
}

func (ctrl *controller) processQueue(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.logger.Errorf("splitting key into namespace and name, error %s", err)
		return err
	}
	ctrl.logger.Infof("Processing backup(%s) request", name)

	backup, err := ctrl.backupLister.Get(name)
	if err != nil {
		// re enqueue for processing
		return errors.Wrap(err, fmt.Sprintf("error getting backup %s from lister", name))
	}

	backupClone := backup.DeepCopy()

	// setup finalizer if not present
	if isBackupInitNeeded(backupClone) {
		_, err := ctrl.backupInitialize(backupClone)
		if err != nil {
			ctrl.logger.Errorf("failed to initialize finalizer backup(%s)", backupClone.Name)
		}
		return err
	}

	if backupClone.DeletionTimestamp != nil {
		return ctrl.deleteBackup(backupClone)
	}

	return ctrl.syncBackup(backupClone)
}

func (ctrl *controller) syncBackup(backup *kahuapi.Backup) error {
	var backupContext Context
	switch backup.Status.Phase {
	case "", kahuapi.BackupPhaseInit:
		ctrl.logger.Info("Validating backup")
		err := ctrl.validateBackup(backup)
		if err != nil {
			return err
		}

		backup, err = ctrl.updateBackupStatusWithEvent(backup, kahuapi.BackupStatus{
			Phase: kahuapi.BackupPhaseVolume,
		}, v1.EventTypeNormal, string(kahuapi.BackupPhaseInit),
			"Backup validation success")
		if err != nil {
			return err
		}
		fallthrough
	case kahuapi.BackupPhaseVolume:
		// get backup context
		backupContext = newContext(backup.Name, ctrl)
		err := backupContext.Complete(backup)
		if err != nil {
			ctrl.logger.Errorf("Update backup(%s) processing: failed to "+
				"get backup resources", backup.Name)
			return err
		}

		err = ctrl.processVolumeBackup(backup, backupContext)
		if err != nil {
			return err
		}
		// populate all meta service
		backup, err = ctrl.updateBackupStatusWithEvent(backup, kahuapi.BackupStatus{
			Phase: kahuapi.BackupPhaseMetadata,
		}, v1.EventTypeNormal, string(kahuapi.BackupPhaseVolume),
			"Volume backup success")
		if err != nil {
			return err
		}
	case kahuapi.BackupPhaseMetadata:
		// get backup context
		backupContext = newContext(backup.Name, ctrl)
		err := backupContext.Complete(backup)
		if err != nil {
			ctrl.logger.Errorf("Update backup(%s) processing: failed to "+
				"get backup resources", backup.Name)
			return err
		}

		err = ctrl.processMetadataBackup(backup, backupContext)
		if err != nil {
			return err
		}
		// populate all meta service
		backup, err = ctrl.updateBackupStatusWithEvent(backup, kahuapi.BackupStatus{
			Phase: kahuapi.BackupPhaseCompleted,
		}, v1.EventTypeNormal, string(kahuapi.BackupPhaseMetadata),
			"Metdata backup success")
		if err != nil {
			return err
		}
	case kahuapi.BackupPhaseCompleted:
		ctrl.logger.Infof("Update backup(%s) processing: ignoring backup %s state", backup.Name,
			backup.Status.Phase)
		return nil
	default:
		ctrl.logger.Infof("Update backup(%s) processing: ignoring backup %s state", backup.Name,
			backup.Status.Phase)
		return nil
	}

	return nil
}

func (ctrl *controller) processVolumeBackup(backup *kahuapi.Backup, ctx Context) error {
	// process backup for metadata and volume backup
	ctrl.logger.Infof("Processing Volume backup(%s)", backup.Name)
	return nil
}

func (ctrl *controller) processMetadataBackup(backup *kahuapi.Backup, ctx Context) error {
	// process backup for metadata and volume backup
	ctrl.logger.Infof("Processing Volume backup(%s)", backup.Name)
	return nil
}

func (ctrl *controller) deleteBackup(backup *kahuapi.Backup) error {
	ctrl.logger.Infof("Processing backup delete request %s", backup.Name)

	if utils.ContainsFinalizer(backup, backupFinalizer) {
		backupClone := backup.DeepCopy()
		utils.RemoveFinalizer(backupClone, backupFinalizer)
		_, err := ctrl.updateBackup(backup, backupClone)
		if err != nil {
			ctrl.logger.Errorf("removing finalizer failed for %s", backup.Name)
		}
		return err
	}
	return nil
}

func (ctrl *controller) validateBackup(backup *kahuapi.Backup) error {
	var validationErrors []string
	// namespace validation
	includeNamespaces := sets.NewString(backup.Spec.IncludeNamespaces...)
	excludeNamespaces := sets.NewString(backup.Spec.ExcludeNamespaces...)
	// check common namespace name in include/exclude list
	if intersection := includeNamespaces.Intersection(excludeNamespaces); intersection.Len() > 0 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("common namespace name (%s) in include and exclude namespace list",
				strings.Join(intersection.List(), ",")))
	}

	// resource validation
	// include resource validation
	for _, resourceSpec := range backup.Spec.IncludeResources {
		if _, err := regexp.Compile(resourceSpec.Name); err != nil {
			validationErrors = append(validationErrors,
				fmt.Sprintf("invalid include resource expression (%s)", resourceSpec.Name))
		}
	}

	// exclude resource validation
	for _, resourceSpec := range backup.Spec.ExcludeResources {
		if _, err := regexp.Compile(resourceSpec.Name); err != nil {
			validationErrors = append(validationErrors,
				fmt.Sprintf("invalid exclude resource expression (%s)", resourceSpec.Name))
		}
	}

	if len(validationErrors) == 0 {
		return nil
	}

	ctrl.logger.Errorf("Backup validation failed. %s", strings.Join(validationErrors, ", "))
	_, err := ctrl.updateBackupStatusWithEvent(backup, kahuapi.BackupStatus{
		Phase:            kahuapi.BackupPhaseCompleted,
		State:            kahuapi.BackupStateFailedValidation,
		ValidationErrors: validationErrors,
	}, v1.EventTypeWarning, string(kahuapi.BackupStateFailedValidation),
		fmt.Sprintf("Backup validation failed. %s", strings.Join(validationErrors, ", ")))

	return errors.Wrap(err, "backup validation failed")
}

func isBackupInitNeeded(backup *kahuapi.Backup) bool {
	if !utils.ContainsFinalizer(backup, backupFinalizer) ||
		backup.Status.Phase == "" {
		return true
	}

	return false
}

func (ctrl *controller) backupInitialize(backup *kahuapi.Backup) (*kahuapi.Backup, error) {
	backupClone := backup.DeepCopy()
	var err error

	if !utils.ContainsFinalizer(backup, backupFinalizer) {
		utils.SetFinalizer(backupClone, backupFinalizer)
		backup, err = ctrl.updateBackup(backup, backupClone)
		if err != nil {
			return backupClone, err
		}
	}

	backupStatus := kahuapi.BackupStatus{}
	if backup.Status.Phase == "" {
		backupStatus.Phase = kahuapi.BackupPhaseInit
		if backup.Status.StartTimestamp == nil {
			time := metav1.Now()
			backupStatus.StartTimestamp = &time
		}
		backup, err = ctrl.updateBackupStatus(backup, backupStatus)
		if err != nil {
			return backup, err
		}
	}

	return backup, nil
}

func (ctrl *controller) updateBackup(oldBackup, newBackup *kahuapi.Backup) (*kahuapi.Backup, error) {
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

	updatedBackup, err := ctrl.backupClient.Patch(context.TODO(),
		oldBackup.Name,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error patching backup")
	}

	return updatedBackup, nil
}

func (ctrl *controller) updateBackupStatus(
	backup *kahuapi.Backup,
	status kahuapi.BackupStatus) (*kahuapi.Backup, error) {

	backupClone := backup.DeepCopy()

	// update Phase
	if backup.Status.Phase != status.Phase {
		backupClone.Status.Phase = status.Phase
	}

	// update Validation error
	backupClone.Status.ValidationErrors = append(backup.Status.ValidationErrors,
		status.ValidationErrors...)

	// update Start time
	if backup.Status.StartTimestamp == nil {
		backupClone.Status.StartTimestamp = status.StartTimestamp
	}

	if backup.Status.LastBackup == nil &&
		status.LastBackup != nil {
		backupClone.Status.LastBackup = status.LastBackup
	}

	newBackup, err := ctrl.backupClient.UpdateStatus(context.TODO(), backupClone, metav1.UpdateOptions{})
	if err != nil {
		ctrl.logger.Errorf("updating backup(%s) status: update status failed %s", backup.Name, err)
	}

	return newBackup, err
}

func (ctrl *controller) updateBackupStatusWithEvent(
	backup *kahuapi.Backup,
	status kahuapi.BackupStatus,
	eventType, reason, message string) (*kahuapi.Backup, error) {

	newBackup, err := ctrl.updateBackupStatus(backup, status)
	if err != nil {
		return newBackup, err
	}

	ctrl.logger.Infof("backup %s changed phase to %q: %s", backup.Name, status.Phase, message)
	ctrl.eventRecorder.Event(newBackup, eventType, reason, message)
	return newBackup, err
}
