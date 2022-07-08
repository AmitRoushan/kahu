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
	"sort"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/soda-cdm/kahu/client/informers/externalversions"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
	"github.com/soda-cdm/kahu/utils"
)

const (
	controllerName  = "backup-controller"
	backupFinalizer = "kahu.io/backup-protection"
)

type Config struct {
	MetaServicePort    uint
	MetaServiceAddress string
}

type controller struct {
	config               *Config
	logger               log.FieldLogger
	genericController    controllers.Controller
	kubeClient           kubernetes.Interface
	dynamicClient        dynamic.Interface
	backupClient         kahuv1client.BackupInterface
	backupLister         kahulister.BackupLister
	backupLocationLister kahulister.BackupLocationLister
	eventRecorder        record.EventRecorder
	backupCache          cache.Store
}

func NewController(config *Config,
	kubeClient kubernetes.Interface,
	kahuClient versioned.Interface,
	dynamicClient dynamic.Interface,
	informer externalversions.SharedInformerFactory,
	eventBroadcaster record.EventBroadcaster) (controllers.Controller, error) {

	logger := log.WithField("controller", controllerName)
	backupController := &controller{
		config:               config,
		logger:               logger,
		kubeClient:           kubeClient,
		backupClient:         kahuClient.KahuV1beta1().Backups(),
		backupLister:         informer.Kahu().V1beta1().Backups().Lister(),
		backupLocationLister: informer.Kahu().V1beta1().BackupLocations().Lister(),
		dynamicClient:        dynamicClient,
		backupCache:          cache.NewStore(cache.MetaNamespaceKeyFunc),
	}

	// register to informer to receive events and push events to worker queue
	informer.Kahu().
		V1beta1().
		Backups().
		Informer().
		AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc:    backupController.handleAdd,
				UpdateFunc: backupController.handleUpdate,
				DeleteFunc: backupController.handleDel,
			},
		)

	// construct controller interface to process worker queue
	genericController, err := controllers.NewControllerBuilder(controllerName).
		SetLogger(logger).
		SetHandler(backupController.processQueue).
		Build()
	if err != nil {
		return nil, err
	}

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
	if err == nil && backup.DeletionTimestamp == nil {
		// process create and sync
		err := ctrl.backupCache.Add(backup)
		if err != nil {
			return err
		}

		// add finalizer
		err = ctrl.ensureFinalizer(backup.Name)
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}
		return nil
		// return ctrl.processBackup(backup)
	}
	if !apierrors.IsNotFound(err) {
		// re enqueue for processing
		return fmt.Errorf("error getting backup %s from informer", name)
	}

	// if not in lister, delete event
	// TODO(Amit Roushan): check with finalizer for delete event
	var (
		exist     bool
		backupObj interface{}
	)
	if backup == nil {
		ctrl.logger.Warningf("Backup%s not available in lister. Checking in local cache", name)
		backupObj, exist, err = ctrl.backupCache.GetByKey(key)
		if err != nil {
			return err
		}
		if !exist {
			// already processed by controller
			return nil
		}
		backup = backupObj.(*kahuapi.Backup)
	}
	return ctrl.processDeleteBackup(backup)
}

func (ctrl *controller) processDeleteBackup(backup *kahuapi.Backup) error {
	ctrl.logger.Infof("Processing backup(%s) request", backup.Name)
	return nil
}

func (ctrl *controller) ensureFinalizer(backupName string) error {
	ctrl.logger.Infof("Ensuring finalizer for backup(%s) request", backupName)

	backup, err := ctrl.backupLister.Get(backupName)
	if err != nil {
		return err
	}

	finalizers := sets.NewString(backup.Finalizers...)
	if !finalizers.Has(backupFinalizer) {
		origBytes, err := json.Marshal(backup)
		if err != nil {
			return errors.Wrap(err, "error marshalling original backup")
		}

		backupCopy := backup.DeepCopy()
		backupCopy.Finalizers = finalizers.Insert(backupFinalizer).List()
		updatedBytes, err := json.Marshal(backupCopy)
		if err != nil {
			return errors.Wrap(err, "error marshalling updated backup")
		}

		patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
		if err != nil {
			return errors.Wrap(err, "error creating json merge patch for backup")
		}

		_, err = ctrl.backupClient.Patch(context.TODO(),
			backup.Name,
			types.MergePatchType,
			patchBytes,
			metav1.PatchOptions{})
		if err != nil {
			return errors.Wrap(err, "error patching backup")
		}
	}

	return nil
}

func (ctrl *controller) processBackup(backup *kahuapi.Backup) error {
	// setting up backup logger
	logger := ctrl.logger.WithField("backup", backup.Name)
	logger.Info("Validating backup")

	backupCopy := backup.DeepCopy()
	// start validating backup object
	// validate namespace input
	validationErrors := ctrl.validateBackup(backupCopy)
	if len(validationErrors) > 0 {
		errorString := strings.Join(validationErrors, ", ")
		logger.Errorf("Backup validation failed. %s", errorString)
		return fmt.Errorf("backup(%s) validation failed", backupCopy.Name)
	}

	// Validate the Metadatalocation
	backupProvider := backup.Spec.MetadataLocation
	ctrl.logger.Infof("preparing backup for provider: %s ", backupProvider)
	backuplocation, err := ctrl.backupLocationLister.Get(backupProvider)
	if err != nil {
		ctrl.logger.Errorf("failed to validate backup location, reason: %s", err)
		backup.Status.Phase = kahuapi.BackupPhaseFailedValidation
		backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
		ctrl.updateStatus(backup, ctrl.backupClient, backup.Status.Phase)
		return err
	}
	ctrl.logger.Debugf("the provider name in backuplocation:%s", backuplocation)

	ctrl.logger.Infof("Preparing backup request for Provider:%s", backupProvider)
	prepareBackupReq := ctrl.prepareBackupRequest(backup)

	if len(prepareBackupReq.Status.ValidationErrors) > 0 {
		prepareBackupReq.Status.Phase = kahuapi.BackupPhaseFailedValidation
		ctrl.updateStatus(prepareBackupReq.Backup, ctrl.backupClient, prepareBackupReq.Status.Phase)
		return err
	} else {
		prepareBackupReq.Status.Phase = kahuapi.BackupPhaseInProgress
	}
	prepareBackupReq.Status.StartTimestamp = &metav1.Time{Time: time.Now()}
	ctrl.updateStatus(prepareBackupReq.Backup, ctrl.backupClient, prepareBackupReq.Status.Phase)

	// start taking backup
	err = ctrl.runBackup(prepareBackupReq)
	if err != nil {
		prepareBackupReq.Status.Phase = kahuapi.BackupPhaseFailed
	} else {
		prepareBackupReq.Status.Phase = kahuapi.BackupPhaseCompleted
	}
	prepareBackupReq.Status.LastBackup = &metav1.Time{Time: time.Now()}

	ctrl.logger.Infof("completed backup with status: %s", prepareBackupReq.Status.Phase)
	ctrl.updateStatus(prepareBackupReq.Backup, ctrl.backupClient, prepareBackupReq.Status.Phase)
	return err
}

func (ctx *controller) validateBackup(backup *kahuapi.Backup) []string {
	var errors []string
	// namespace validation
	includeNamespaces := sets.NewString(backup.Spec.IncludeNamespaces...)
	excludeNamespaces := sets.NewString(backup.Spec.ExcludeNamespaces...)
	// check common namespace name in include/exclude list
	if intersection := includeNamespaces.Intersection(excludeNamespaces); intersection.Len() > 0 {
		errors = append(errors,
			fmt.Sprintf("common namespace name (%s) in include and exclude namespace list",
				strings.Join(intersection.List(), ",")))
	}

	// resource validation
	includeResources := sets.NewString(backup.Spec.IncludeResources...)
	excludeResources := sets.NewString(backup.Spec.ExcludeResources...)
	// check common namespace name in include/exclude list
	if intersection := includeResources.Intersection(excludeResources); intersection.Len() > 0 {
		errors = append(errors,
			fmt.Sprintf("common resource name (%s) in include and exclude resource list",
				strings.Join(intersection.List(), ",")))
	}

	return errors
}

func (ctrl *controller) prepareBackupRequest(backup *kahuapi.Backup) *PrepareBackup {
	backupRequest := &PrepareBackup{
		Backup: backup.DeepCopy(),
	}

	if backupRequest.Annotations == nil {
		backupRequest.Annotations = make(map[string]string)
	}

	if backupRequest.Labels == nil {
		backupRequest.Labels = make(map[string]string)
	}

	// validate the resources from include and exlude list
	for _, err := range utils.ValidateIncludesExcludes(backupRequest.Spec.IncludeResources, backupRequest.Spec.ExcludeResources) {
		backupRequest.Status.ValidationErrors = append(backupRequest.Status.ValidationErrors, fmt.Sprintf("Include/Exclude resourse list is not valid: %v", err))
	}

	// validate the namespace from include and exlude list
	for _, err := range utils.ValidateNamespace(backupRequest.Spec.IncludeNamespaces, backupRequest.Spec.ExcludeNamespaces) {
		backupRequest.Status.ValidationErrors = append(backupRequest.Status.ValidationErrors, fmt.Sprintf("Include/Exclude namespace list is not valid: %v", err))
	}

	var allNamespace []string
	if len(backupRequest.Spec.IncludeNamespaces) == 0 {
		allNamespace, _ = ctrl.ListNamespaces(backupRequest)
	}
	ResultantNamespace = utils.GetResultantItems(allNamespace, backupRequest.Spec.IncludeNamespaces, backupRequest.Spec.ExcludeNamespaces)

	ResultantResource = utils.GetResultantItems(utils.SupportedResourceList, backupRequest.Spec.IncludeResources, backupRequest.Spec.ExcludeResources)

	// till now validation is ok. Set the backupphase as New to start backup
	backupRequest.Status.Phase = kahuapi.BackupPhaseInit

	return backupRequest
}

func (ctrl *controller) updateStatus(bkp *kahuapi.Backup, client kahuv1client.BackupInterface, phase kahuapi.BackupPhase) {
	backup, err := client.Get(context.Background(), bkp.Name, metav1.GetOptions{})
	if err != nil {
		ctrl.logger.Errorf("failed to get backup for updating status :%+s", err)
		return
	}

	if backup.Status.Phase == kahuapi.BackupPhaseCompleted && phase == kahuapi.BackupPhaseFailed {
		backup.Status.Phase = kahuapi.BackupPhasePartiallyFailed
	} else if backup.Status.Phase == kahuapi.BackupPhasePartiallyFailed {
		backup.Status.Phase = kahuapi.BackupPhasePartiallyFailed
	} else {
		backup.Status.Phase = phase
	}
	backup.Status.ValidationErrors = bkp.Status.ValidationErrors
	_, err = client.UpdateStatus(context.Background(), backup, metav1.UpdateOptions{})
	if err != nil {
		ctrl.logger.Errorf("failed to update backup status :%+s", err)
	}

	return
}

func (ctrl *controller) getGVR(input string) (GroupResouceVersion, error) {
	var gvr GroupResouceVersion

	_, resource, _ := ctrl.kubeClient.Discovery().ServerGroupsAndResources()

	for _, group := range resource {
		// Parse so we can check if this is the core group
		gv, err := schema.ParseGroupVersion(group.GroupVersion)
		if err != nil {
			return gvr, err
		}
		if gv.Group == "" {
			sortCoreGroup(group)
		}

		for _, resource := range group.APIResources {
			gvr = ctrl.getResourceItems(gv, resource, input)
			if gvr.resourceName == input {
				return gvr, nil
			}
		}

	}
	return gvr, nil
}

func (ctrl *controller) runBackup(backup *PrepareBackup) error {
	ctrl.logger.Infoln("Starting to run backup")

	backupClient := utils.GetMetaserviceBackupClient(ctrl.config.MetaServiceAddress, ctrl.config.MetaServicePort)

	err := backupClient.Send(&metaservice.BackupRequest{
		Backup: &metaservice.BackupRequest_Identifier{
			Identifier: &metaservice.BackupIdentifier{
				BackupHandle: backup.Name,
			},
		},
	})

	if err != nil {
		ctrl.logger.Errorf("Unable to connect metadata service %s", err)
		return err
	}

	resultantResource := sets.NewString(ResultantResource...)
	resultantNamespace := sets.NewString(ResultantNamespace...)
	ctrl.logger.Infof("backup will be taken for these resources:%s", resultantResource)
	ctrl.logger.Infof("backup will be taken for these namespaces:%s", resultantNamespace)

	for ns, nsVal := range resultantNamespace {
		ctrl.logger.Infof("started backup for namespace:%s", ns)
		for name, val := range resultantResource {
			ctrl.logger.Debug(nsVal, val)
			switch name {
			case "deployments":
				gvr, err := ctrl.getGVR("deployments")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.deploymentBackup(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
				ctrl.updateStatus(backup.Backup, ctrl.backupClient, backup.Status.Phase)
			case "configmaps":
				gvr, err := ctrl.getGVR("configmaps")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.getConfigMapS(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "persistentvolumeclaims":
				gvr, err := ctrl.getGVR("persistentvolumeclaims")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.getPersistentVolumeClaims(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "storageclasses":
				gvr, err := ctrl.getGVR("storageclasses")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.getStorageClass(gvr, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "services":
				gvr, err := ctrl.getGVR("services")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.getServices(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "secrets":
				gvr, err := ctrl.getGVR("secrets")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.getSecrets(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "endpoints":
				gvr, err := ctrl.getGVR("endpoints")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.getEndpoints(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "replicasets":
				gvr, err := ctrl.getGVR("replicasets")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.getReplicasets(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "statefulsets":
				gvr, err := ctrl.getGVR("statefulsets")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = ctrl.getStatefulsets(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			default:
				continue
			}
		}
	}
	_, err = backupClient.CloseAndRecv()

	return err
}

func (ctrl *controller) getResourceObjects(backup *PrepareBackup,
	gvr GroupResouceVersion, ns string,
	labelSelectors map[string]string) (*unstructured.UnstructuredList, error) {

	resGVR := schema.GroupVersionResource{
		Group:    gvr.group,
		Version:  gvr.version,
		Resource: gvr.resourceName,
	}
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	var (
		objectList *unstructured.UnstructuredList
		err        error
	)
	selectors := labels.Set(labelSelectors).String()

	if ns != "" {
		objectList, err = ctrl.dynamicClient.Resource(resGVR).
			Namespace(ns).
			List(context.Background(), metav1.ListOptions{
				LabelSelector: selectors,
			})
	} else {
		objectList, err = ctrl.dynamicClient.Resource(resGVR).
			List(context.Background(), metav1.ListOptions{})
	}
	return objectList, err
}

// getResourceItems collects all relevant items for a given group-version-resource.
func (ctrl *controller) getResourceItems(gv schema.GroupVersion, resource metav1.APIResource, input string) GroupResouceVersion {
	gvr := gv.WithResource(resource.Name)

	var groupResourceVersion GroupResouceVersion
	if input == gvr.Resource {
		groupResourceVersion = GroupResouceVersion{
			resourceName: gvr.Resource,
			version:      gvr.Version,
			group:        gvr.Group,
		}
	}
	return groupResourceVersion
}

// sortCoreGroup sorts the core API group.
func sortCoreGroup(group *metav1.APIResourceList) {
	sort.SliceStable(group.APIResources, func(i, j int) bool {
		return CoreGroupResourcePriority(group.APIResources[i].Name) < CoreGroupResourcePriority(group.APIResources[j].Name)
	})
}

func (ctrl *controller) backupSend(obj runtime.Object, metadataName string,
	backupSendClient metaservice.MetaService_BackupClient) error {

	gvk, err := addTypeInformationToObject(obj)
	if err != nil {
		ctrl.logger.Errorf("Unable to get resource content: %s", err)
		return err
	}

	resourceData, err := json.Marshal(obj)
	if err != nil {
		ctrl.logger.Errorf("Unable to get resource content: %s", err)
		return err
	}

	ctrl.logger.Infof("sending metadata for object %s/%s", gvk, metadataName)

	err = backupSendClient.Send(&metaservice.BackupRequest{
		Backup: &metaservice.BackupRequest_BackupResource{
			BackupResource: &metaservice.BackupResource{
				Resource: &metaservice.Resource{
					Name:    metadataName,
					Group:   gvk.Group,
					Version: gvk.Version,
					Kind:    gvk.Kind,
				},
				Data: resourceData,
			},
		},
	})
	return err
}

func (ctrl *controller) deleteBackup(name string, backup *kahuapi.Backup) {
	// TODO: delete need to be added
	ctrl.logger.Infof("delete is called for backup:%s", name)

}

func (ctrl *controller) handleAdd(obj interface{}) {
	backup := obj.(*kahuapi.Backup)

	switch backup.Status.Phase {
	case "", kahuapi.BackupPhaseInit:
	default:
		ctrl.logger.WithFields(log.Fields{
			"backup": backup.Name,
			"phase":  backup.Status.Phase,
		}).Infof("Backup: %s is not New, so will not be processed", backup.Name)
		return
	}
	ctrl.genericController.Enqueue(obj)
}

func (ctrl *controller) handleDel(obj interface{}) {
	ctrl.genericController.Enqueue(obj)
}

func (ctrl *controller) handleUpdate(oldObject, newObject interface{}) {
	ctrl.genericController.Enqueue(newObject)
}

// addTypeInformationToObject adds TypeMeta information to a runtime.Object based upon the loaded scheme.Scheme
// inspired by: https://github.com/kubernetes/cli-runtime/blob/v0.19.2/pkg/printers/typesetter.go#L41
func addTypeInformationToObject(obj runtime.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := scheme.Scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("missing apiVersion or kind and cannot assign it; %w", err)
	}

	for _, gvk := range gvks {
		if len(gvk.Kind) == 0 {
			continue
		}
		if len(gvk.Version) == 0 || gvk.Version == runtime.APIVersionInternal {
			continue
		}

		obj.GetObjectKind().SetGroupVersionKind(gvk)
		return gvk, nil
	}

	return schema.GroupVersionKind{}, err
}
