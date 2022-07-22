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
	"github.com/pkg/errors"
	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
	"github.com/soda-cdm/kahu/utils"
	"google.golang.org/grpc"
	"io"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	kahuclient "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client/informers/externalversions"
	kahulister "github.com/soda-cdm/kahu/client/listers/kahu/v1beta1"
	"github.com/soda-cdm/kahu/controllers"
	"github.com/soda-cdm/kahu/discovery"
)

type controller struct {
	logger               log.FieldLogger
	genericController    controllers.Controller
	kubeClient           kubernetes.Interface
	dynamicClient        dynamic.Interface
	restoreClient        kahuclient.RestoreInterface
	discoveryHelper      discovery.DiscoveryHelper
	restoreLister        kahulister.RestoreLister
	backupLister         kahulister.BackupLister
	backupLocationLister kahulister.BackupLocationLister
	providerLister       kahulister.ProviderLister
	restoreVolumeClient  kahuclient.VolumeRestoreContentInterface
}

func NewController(kubeClient kubernetes.Interface,
	kahuClient versioned.Interface,
	dynamicClient dynamic.Interface,
	discoveryHelper discovery.DiscoveryHelper,
	informer externalversions.SharedInformerFactory) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	restoreController := &controller{
		logger:               logger,
		kubeClient:           kubeClient,
		dynamicClient:        dynamicClient,
		discoveryHelper:      discoveryHelper,
		restoreClient:        kahuClient.KahuV1beta1().Restores(),
		restoreLister:        informer.Kahu().V1beta1().Restores().Lister(),
		backupLister:         informer.Kahu().V1beta1().Backups().Lister(),
		backupLocationLister: informer.Kahu().V1beta1().BackupLocations().Lister(),
		providerLister:       informer.Kahu().V1beta1().Providers().Lister(),
		restoreVolumeClient:  kahuClient.KahuV1beta1().VolumeRestoreContents(),
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
	informer.Kahu().V1beta1().Restores().Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: genericController.Enqueue,
			UpdateFunc: func(oldObj, newObj interface{}) {
				genericController.Enqueue(newObj)
			},
		},
	)

	// reference back
	restoreController.genericController = genericController
	return genericController, err
}

type restoreContext struct {
	logger               log.FieldLogger
	kubeClient           kubernetes.Interface
	dynamicClient        dynamic.Interface
	discoveryHelper      discovery.DiscoveryHelper
	restoreClient        kahuclient.RestoreInterface
	restoreLister        kahulister.RestoreLister
	backupLister         kahulister.BackupLister
	backupLocationLister kahulister.BackupLocationLister
	providerLister       kahulister.ProviderLister
	restoreVolumeClient  kahuclient.VolumeRestoreContentInterface
	backupObjectIndexer  cache.Indexer
	filter               filterHandler
	mutator              mutationHandler
	resolver             Interface
}

func newRestoreContext(name string, ctrl *controller) *restoreContext {
	backupObjectIndexer := cache.NewIndexer(uidKeyFunc, newBackupObjectIndexers())
	logger := ctrl.logger.WithField("restore", name)
	return &restoreContext{
		logger:               logger,
		kubeClient:           ctrl.kubeClient,
		restoreClient:        ctrl.restoreClient,
		restoreLister:        ctrl.restoreLister,
		backupLister:         ctrl.backupLister,
		backupLocationLister: ctrl.backupLocationLister,
		providerLister:       ctrl.providerLister,
		dynamicClient:        ctrl.dynamicClient,
		discoveryHelper:      ctrl.discoveryHelper,
		backupObjectIndexer:  backupObjectIndexer,
		restoreVolumeClient:  ctrl.restoreVolumeClient,
		filter:               constructFilterHandler(logger.WithField("context", "filter")),
		mutator:              constructMutationHandler(logger.WithField("context", "mutator")),
		resolver:             NewResolver(logger.WithField("context", "resolver")),
	}
}

func uidKeyFunc(obj interface{}) (string, error) {
	if key, ok := obj.(cache.ExplicitKey); ok {
		return string(key), nil
	}
	meta, err := meta.Accessor(obj)
	if err != nil {
		return "", fmt.Errorf("object has no meta: %v", err)
	}

	return string(meta.GetUID()), nil
}

func newBackupObjectIndexers() cache.Indexers {
	return cache.Indexers{
		backupObjectNamespaceIndex: func(obj interface{}) ([]string, error) {
			keys := make([]string, 0)
			metadata, err := meta.Accessor(obj)
			if err != nil {
				return nil, fmt.Errorf("object has no meta: %v", err)
			}
			if len(metadata.GetNamespace()) > 0 {
				keys = append(keys, metadata.GetNamespace())
			}
			return keys, nil
		},
		backupObjectClusterResourceIndex: func(obj interface{}) ([]string, error) {
			keys := make([]string, 0)
			metadata, err := meta.Accessor(obj)
			if err != nil {
				return nil, fmt.Errorf("object has no meta: %v", err)
			}
			if len(metadata.GetNamespace()) == 0 {
				keys = append(keys, metadata.GetName())
			}
			return keys, nil
		},
		backupObjectResourceIndex: func(obj interface{}) ([]string, error) {
			keys := make([]string, 0)
			switch u := obj.(type) {
			case runtime.Unstructured:
				keys = append(keys, u.GetObjectKind().GroupVersionKind().Kind)
			default:
				log.Warnf("%v is not unstructred object. %s skipped", obj,
					backupObjectResourceIndex)
			}

			return keys, nil
		},
	}
}

func (ctrl *controller) processQueue(index string) error {
	// get restore name
	_, name, err := cache.SplitMetaNamespaceKey(index)
	if err != nil {
		ctrl.logger.Errorf("splitting key into namespace and name, error %s", err)
		return err
	}

	// get restore from cache
	restore, err := ctrl.restoreLister.Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctrl.logger.Debugf("restore %s not found", name)
		}
		ctrl.logger.Errorf("error %s, Getting the restore resource from lister", err)
		return err
	}

	// avoid polluting restore object
	workCopyRestore := restore.DeepCopy()
	restoreCtx := newRestoreContext(workCopyRestore.Name, ctrl)

	if restore.DeletionTimestamp != nil {
		return restoreCtx.deleteRestore(workCopyRestore)
	}

	// update start time
	if workCopyRestore.Status.StartTimestamp.IsZero() {
		time := metav1.Now()
		workCopyRestore.Status.StartTimestamp = &time
		if workCopyRestore.Status.Stage == "" {
			workCopyRestore.Status.State = kahuapi.RestoreStateProcessing
			workCopyRestore.Status.Stage = kahuapi.RestoreStageInitial
		}
		workCopyRestore, err = restoreCtx.updateRestoreStatus(workCopyRestore)
		if err != nil {
			ctrl.logger.Errorf("Unable to update initial restore(%s) state", index)
			return err
		}
	}

	return restoreCtx.syncRestore(workCopyRestore)
}

func (ctx *restoreContext) deleteRestore(restore *kahuapi.Restore) error {
	ctx.logger.Infof("Processing delete request for %s", restore.Name)
	return nil
}

// syncRestore validates and perform restoration of resources
// Restoration process are divided into multiple steps and each step are based on restore phases
// Only restore object are passed on in Phases. The mechanism will help in failure/crash scenario
func (ctx *restoreContext) syncRestore(restore *kahuapi.Restore) error {
	var err error
	ctx.logger.Infof("Processing restore for %s", restore.Name)
	// handle restore with stages
	switch restore.Status.Stage {
	case "", kahuapi.RestoreStageInitial:
		ctx.logger.Infof("Restore in %s phase", kahuapi.RestoreStageInitial)
		// validate restore
		ctx.validateRestore(restore)
		if len(restore.Status.ValidationErrors) > 0 {
			ctx.logger.Errorf("Restore validation failed. %s",
				strings.Join(restore.Status.ValidationErrors, ","))
			restore.Status.State = kahuapi.RestoreStateFailed
			restore, err = ctx.updateRestoreStatus(restore)
			return err
		}

		// validate and fetch backup info
		_, err := ctx.fetchBackupInfo(restore)
		if err != nil {
			// update failure reason
			if len(restore.Status.FailureReason) > 0 {
				restore, err = ctx.updateRestoreStatus(restore)
				return err
			}
			return err
		}

		ctx.logger.Info("Restore specification validation success")
		// update status to metadata restore
		restore.Status.Stage = kahuapi.RestoreStageResources
		restore, err = ctx.updateRestoreStatus(restore)
		if err != nil {
			ctx.logger.Errorf("Restore status update failed. %s", err)
			return err
		}
	}

	// fetch backup info
	backupInfo, err := ctx.fetchBackupInfo(restore)
	if err != nil {
		return err
	}

	// construct backup identifier
	backupIdentifier, err := utils.GetBackupIdentifier(backupInfo.backup,
		backupInfo.backupLocation,
		backupInfo.backupProvider)
	if err != nil {
		return err
	}

	// fetch backup content and cache them
	err = ctx.fetchBackupContent(backupInfo.backupProvider, backupIdentifier, restore)
	if err != nil {
		restore.Status.State = kahuapi.RestoreStateFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to get backup content. %s",
			err)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	restoreIndexer, err := ctx.cloneIndexer()
	if err != nil {
		return err
	}

	// filter resources from cache
	err = ctx.filter.handle(restore, restoreIndexer)
	if err != nil {
		restore.Status.State = kahuapi.RestoreStateFailed
		errMsg := fmt.Sprintf("Failed to filter resources. %s", err)
		restore.Status.FailureReason = errMsg
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	// resolve dependencies for restore candidates
	err = ctx.resolver.Resolve(restoreIndexer, ctx.backupObjectIndexer)
	if err != nil {
		restore.Status.State = kahuapi.RestoreStateFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to get dependency resources. %s", err)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	// add mutation
	err = ctx.mutator.handle(restore, restoreIndexer)
	if err != nil {
		restore.Status.State = kahuapi.RestoreStateFailed
		restore.Status.FailureReason = fmt.Sprintf("Failed to mutate resources. %s", err)
		restore, err = ctx.updateRestoreStatus(restore)
		return err
	}

	return ctx.syncVolumeRestore(restore, restoreIndexer)
}

func (ctx *restoreContext) validateRestore(restore *kahuapi.Restore) {
	// namespace validation
	includeNamespaces := sets.NewString(restore.Spec.IncludeNamespaces...)
	excludeNamespaces := sets.NewString(restore.Spec.ExcludeNamespaces...)
	// check common namespace name in include/exclude list
	if intersection := includeNamespaces.Intersection(excludeNamespaces); intersection.Len() > 0 {
		restore.Status.ValidationErrors =
			append(restore.Status.ValidationErrors,
				fmt.Sprintf("common namespace name (%s) in include and exclude namespace list",
					strings.Join(intersection.List(), ",")))
	}

	// resource validation
	// check regular expression validity
	for _, resourceSpec := range restore.Spec.IncludeResources {
		if _, err := regexp.Compile(resourceSpec.Name); err != nil {
			restore.Status.ValidationErrors =
				append(restore.Status.ValidationErrors,
					fmt.Sprintf("invalid include resource name specification name %s",
						resourceSpec.Name))
		}
	}
	for _, resourceSpec := range restore.Spec.ExcludeResources {
		if _, err := regexp.Compile(resourceSpec.Name); err != nil {
			restore.Status.ValidationErrors =
				append(restore.Status.ValidationErrors,
					fmt.Sprintf("invalid include resource name specification name %s",
						resourceSpec.Name))
		}
	}
}

func (ctx *restoreContext) updateRestoreStatus(restore *kahuapi.Restore) (*kahuapi.Restore, error) {
	// get restore status from lister
	currentRestore := restore.DeepCopy()

	currentRestore.Status = restore.Status
	updatedRestore, err := ctx.restoreClient.UpdateStatus(context.TODO(), currentRestore,
		v1.UpdateOptions{})
	if err != nil {
		if apierrors.IsResourceExpired(err) {
			return restore, fmt.Errorf("Restore resource updated. %s", err)
		}
		return restore, err
	}

	return updatedRestore, err
}

func (ctx *restoreContext) fetchBackup(restore *kahuapi.Restore) (*kahuapi.Backup, error) {
	// fetch backup
	backupName := restore.Spec.BackupName
	backup, err := ctx.backupLister.Get(backupName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctx.logger.Errorf("Backup(%s) do not exist", backupName)
			return nil, err
		}
		ctx.logger.Errorf("Failed to get backup. %s", err)
		return nil, err
	}

	return backup, err
}

func (ctx *restoreContext) fetchBackupLocation(locationName string,
	restore *kahuapi.Restore) (*kahuapi.BackupLocation, error) {
	// fetch backup location
	backupLocation, err := ctx.backupLocationLister.Get(locationName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctx.logger.Errorf("Backup location(%s) do not exist", locationName)
			return nil, err
		}
		ctx.logger.Errorf("Failed to get backup location. %s", err)
		return nil, err
	}

	return backupLocation, err
}

func (ctx *restoreContext) fetchProvider(providerName string,
	restore *kahuapi.Restore) (*kahuapi.Provider, error) {
	// fetch provider
	provider, err := ctx.providerLister.Get(providerName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			ctx.logger.Errorf("Metadata Provider(%s) do not exist", providerName)
			return nil, err
		}
		ctx.logger.Errorf("Failed to get metadata provider. %s", err)
		return nil, err
	}

	return provider, nil
}

func (ctx *restoreContext) fetchBackupInfo(restore *kahuapi.Restore) (*backupInfo, error) {
	backup, err := ctx.fetchBackup(restore)
	if err != nil {
		ctx.logger.Errorf("Failed to get backup information for backup(%s). %s",
			restore.Spec.BackupName, err)
		return nil, err
	}

	backupLocation, err := ctx.fetchBackupLocation(backup.Spec.MetadataLocation, restore)
	if err != nil {
		ctx.logger.Errorf("Failed to get backup location information for %s. %s",
			backup.Spec.MetadataLocation, err)
		return nil, err
	}

	provider, err := ctx.fetchProvider(backupLocation.Spec.ProviderName, restore)
	if err != nil {
		ctx.logger.Errorf("Failed to get backup location provider for %s. %s",
			backupLocation.Spec.ProviderName, err)
		return nil, err
	}

	return &backupInfo{
		backup:         backup,
		backupLocation: backupLocation,
		backupProvider: provider,
	}, nil
}

func (ctx *restoreContext) fetchMetaServiceClient(backupProvider *kahuapi.Provider,
	_ *kahuapi.Restore) (metaservice.MetaServiceClient, *grpc.ClientConn, error) {
	if backupProvider.Spec.Type != kahuapi.ProviderTypeMetadata {
		return nil, nil, fmt.Errorf("invalid metadata provider type (%s)",
			backupProvider.Spec.Type)
	}

	// fetch service name
	providerService, exist := backupProvider.Annotations[utils.BackupLocationServiceAnnotation]
	if !exist {
		return nil, nil, fmt.Errorf("failed to get metadata provider(%s) service info",
			backupProvider.Name)
	}

	metaServiceClient, grpcConn, err := metaservice.GetMetaServiceClient(providerService)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get metadata service client(%s)",
			providerService)
	}

	return metaServiceClient, grpcConn, nil
}

func (ctx *restoreContext) fetchBackupContent(backupProvider *kahuapi.Provider,
	backupIdentifier *metaservice.BackupIdentifier,
	restore *kahuapi.Restore) error {
	// fetch meta service client
	metaServiceClient, grpcConn, err := ctx.fetchMetaServiceClient(backupProvider, restore)
	if err != nil {
		ctx.logger.Errorf("Error fetching meta service client. %s", err)
		return err
	}
	defer grpcConn.Close()

	// fetch metadata backup file
	restoreClient, err := metaServiceClient.Restore(context.TODO(), &metaservice.RestoreRequest{
		Id: backupIdentifier,
	})
	if err != nil {
		ctx.logger.Errorf("Error fetching meta service restore client. %s", err)
		return fmt.Errorf("error fetching meta service restore client. %s", err)
	}
	defer restoreClient.CloseSend()

	for {
		res, err := restoreClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("Failed fetching data. %s", err)
			return errors.Wrap(err, "unable to receive backup resource meta")
		}

		obj := new(unstructured.Unstructured)
		err = json.Unmarshal(res.GetBackupResource().GetData(), obj)
		if err != nil {
			log.Errorf("Failed to unmarshal on backed up data %s", err)
			return errors.Wrap(err, "unable to unmarshal received resource meta")
		}

		ctx.logger.Infof("Received %s/%s from meta service", obj.GroupVersionKind(), obj.GetName())
		if ctx.excludeResource(obj) {
			ctx.logger.Infof("Excluding %s/%s from processing", obj.GroupVersionKind(), obj.GetName())
			continue
		}

		err = ctx.backupObjectIndexer.Add(obj)
		if err != nil {
			ctx.logger.Errorf("Unable to add resource %s/%s in restore cache. %s", obj.GroupVersionKind(),
				obj.GetName(), err)
			return err
		}
	}

	return nil
}

func (ctx *restoreContext) cloneIndexer() (cache.Indexer, error) {
	restoreObjectIndexer := cache.NewIndexer(uidKeyFunc, newBackupObjectIndexers())

	for _, restoreObj := range ctx.backupObjectIndexer.List() {
		err := restoreObjectIndexer.Add(restoreObj)
		if err != nil {
			ctx.logger.Errorf("Unable to clone backup objects for restore. %s", err)
			return restoreObjectIndexer, errors.Wrap(err, "unable to clone backup objects for restore")
		}
	}

	return restoreObjectIndexer, nil
}

func (ctx *restoreContext) excludeResource(resource *unstructured.Unstructured) bool {
	if excludeResources.Has(resource.GetKind()) {
		return true
	}

	switch resource.GetKind() {
	case "Service":
		if resource.GetName() == "kubernetes" {
			return true
		}
	}

	return false
}
