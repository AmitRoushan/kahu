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
	"k8s.io/client-go/dynamic"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/soda-cdm/kahu/client/informers/externalversions"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
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
	controllerName = "BackupController"
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
	}

	// register to informer to receive events and push events to worker queue
	informer.Kahu().
		V1beta1().
		Backups().
		Informer().
		AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc:    backupController.handleAdd,
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
		v1.EventSource{Component: "backup-controller"})
	backupController.eventRecorder = eventRecorder

	// reference back
	backupController.genericController = genericController
	return genericController, err
}

func (c *controller) processQueue(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		c.logger.Errorf("splitting key into namespace and name, error %s", err)
		return err
	}

	backup, err := c.backupLister.Get(name)
	if err == nil {
		// process create and sync
		return c.processBackup(backup)
	}
	if !apierrors.IsNotFound(err) {
		// re enqueue for processing
		return fmt.Errorf("error getting backup %s from informer", name)
	}

	// if not in lister, delete event
	// TODO(Amit Roushan): check with finalizer for delete event
	return err
}

func (c *controller) processBackup(backup *kahuapi.Backup) error {
	// setting up backup logger
	logger := c.logger.WithField("backup", backup.Name)
	logger.Info("Validating backup")

	backupCopy := backup.DeepCopy()
	// start validating backup object
	// validate namespace input
	validationErrors := c.validateBackup(backupCopy)
	if len(validationErrors) > 0 {
		errorString := strings.Join(validationErrors, ", ")
		logger.Errorf("Backup validation failed. %s", errorString)
		return fmt.Errorf("backup(%s) validation failed", backupCopy.Name)
	}

	// Validate the Metadatalocation
	backupProvider := backup.Spec.MetadataLocation
	c.logger.Infof("preparing backup for provider: %s ", backupProvider)
	backuplocation, err := c.backupLocationLister.Get(backupProvider)
	if err != nil {
		c.logger.Errorf("failed to validate backup location, reason: %s", err)
		backup.Status.Phase = kahuapi.BackupPhaseFailedValidation
		backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
		c.updateStatus(backup, c.backupClient, backup.Status.Phase)
		return err
	}
	c.logger.Debugf("the provider name in backuplocation:%s", backuplocation)

	c.logger.Infof("Preparing backup request for Provider:%s", backupProvider)
	prepareBackupReq := c.prepareBackupRequest(backup)

	if len(prepareBackupReq.Status.ValidationErrors) > 0 {
		prepareBackupReq.Status.Phase = kahuapi.BackupPhaseFailedValidation
		c.updateStatus(prepareBackupReq.Backup, c.backupClient, prepareBackupReq.Status.Phase)
		return err
	} else {
		prepareBackupReq.Status.Phase = kahuapi.BackupPhaseInProgress
	}
	prepareBackupReq.Status.StartTimestamp = &metav1.Time{Time: time.Now()}
	c.updateStatus(prepareBackupReq.Backup, c.backupClient, prepareBackupReq.Status.Phase)

	// start taking backup
	err = c.runBackup(prepareBackupReq)
	if err != nil {
		prepareBackupReq.Status.Phase = kahuapi.BackupPhaseFailed
	} else {
		prepareBackupReq.Status.Phase = kahuapi.BackupPhaseCompleted
	}
	prepareBackupReq.Status.LastBackup = &metav1.Time{Time: time.Now()}

	c.logger.Infof("completed backup with status: %s", prepareBackupReq.Status.Phase)
	c.updateStatus(prepareBackupReq.Backup, c.backupClient, prepareBackupReq.Status.Phase)
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

func (c *controller) prepareBackupRequest(backup *kahuapi.Backup) *PrepareBackup {
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
		allNamespace, _ = c.ListNamespaces(backupRequest)
	}
	ResultantNamespace = utils.GetResultantItems(allNamespace, backupRequest.Spec.IncludeNamespaces, backupRequest.Spec.ExcludeNamespaces)

	ResultantResource = utils.GetResultantItems(utils.SupportedResourceList, backupRequest.Spec.IncludeResources, backupRequest.Spec.ExcludeResources)

	// till now validation is ok. Set the backupphase as New to start backup
	backupRequest.Status.Phase = kahuapi.BackupPhaseInit

	return backupRequest
}

func (c *controller) updateStatus(bkp *kahuapi.Backup, client kahuv1client.BackupInterface, phase kahuapi.BackupPhase) {
	backup, err := client.Get(context.Background(), bkp.Name, metav1.GetOptions{})
	if err != nil {
		c.logger.Errorf("failed to get backup for updating status :%+s", err)
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
		c.logger.Errorf("failed to update backup status :%+s", err)
	}

	return
}

func (c *controller) getGVR(input string) (GroupResouceVersion, error) {
	var gvr GroupResouceVersion

	_, resource, _ := c.kubeClient.Discovery().ServerGroupsAndResources()

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
			gvr = c.getResourceItems(gv, resource, input)
			if gvr.resourceName == input {
				return gvr, nil
			}
		}

	}
	return gvr, nil
}

func (c *controller) runBackup(backup *PrepareBackup) error {
	c.logger.Infoln("Starting to run backup")

	backupClient := utils.GetMetaserviceBackupClient(c.config.MetaServiceAddress, c.config.MetaServicePort)

	err := backupClient.Send(&metaservice.BackupRequest{
		Backup: &metaservice.BackupRequest_Identifier{
			Identifier: &metaservice.BackupIdentifier{
				BackupHandle: backup.Name,
			},
		},
	})

	if err != nil {
		c.logger.Errorf("Unable to connect metadata service %s", err)
		return err
	}

	resultantResource := sets.NewString(ResultantResource...)
	resultantNamespace := sets.NewString(ResultantNamespace...)
	c.logger.Infof("backup will be taken for these resources:%s", resultantResource)
	c.logger.Infof("backup will be taken for these namespaces:%s", resultantNamespace)

	for ns, nsVal := range resultantNamespace {
		c.logger.Infof("started backup for namespace:%s", ns)
		for name, val := range resultantResource {
			c.logger.Debug(nsVal, val)
			switch name {
			case "deployments":
				gvr, err := c.getGVR("deployments")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.deploymentBackup(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
				c.updateStatus(backup.Backup, c.backupClient, backup.Status.Phase)
			case "configmaps":
				gvr, err := c.getGVR("configmaps")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.getConfigMapS(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "persistentvolumeclaims":
				gvr, err := c.getGVR("persistentvolumeclaims")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.getPersistentVolumeClaims(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "storageclasses":
				gvr, err := c.getGVR("storageclasses")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.getStorageClass(gvr, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "services":
				gvr, err := c.getGVR("services")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.getServices(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "secrets":
				gvr, err := c.getGVR("secrets")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.getSecrets(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "endpoints":
				gvr, err := c.getGVR("endpoints")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.getEndpoints(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "replicasets":
				gvr, err := c.getGVR("replicasets")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.getReplicasets(gvr, ns, backup, backupClient)
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				} else {
					backup.Status.Phase = kahuapi.BackupPhaseCompleted
				}
			case "statefulsets":
				gvr, err := c.getGVR("statefulsets")
				if err != nil {
					backup.Status.Phase = kahuapi.BackupPhaseFailed
				}
				err = c.getStatefulsets(gvr, ns, backup, backupClient)
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

func (c *controller) getResourceObjects(backup *PrepareBackup,
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
		objectList, err = c.dynamicClient.Resource(resGVR).
			Namespace(ns).
			List(context.Background(), metav1.ListOptions{
				LabelSelector: selectors,
			})
	} else {
		objectList, err = c.dynamicClient.Resource(resGVR).
			List(context.Background(), metav1.ListOptions{})
	}
	return objectList, err
}

// getResourceItems collects all relevant items for a given group-version-resource.
func (c *controller) getResourceItems(gv schema.GroupVersion, resource metav1.APIResource, input string) GroupResouceVersion {
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

func (c *controller) backupSend(obj runtime.Object, metadataName string,
	backupSendClient metaservice.MetaService_BackupClient) error {

	gvk, err := addTypeInformationToObject(obj)
	if err != nil {
		c.logger.Errorf("Unable to get resource content: %s", err)
		return err
	}

	resourceData, err := json.Marshal(obj)
	if err != nil {
		c.logger.Errorf("Unable to get resource content: %s", err)
		return err
	}

	c.logger.Infof("sending metadata for object %s/%s", gvk, metadataName)

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

func (c *controller) deleteBackup(name string, backup *kahuapi.Backup) {
	// TODO: delete need to be added
	c.logger.Infof("delete is called for backup:%s", name)

}

func (c *controller) handleAdd(obj interface{}) {
	backup := obj.(*kahuapi.Backup)

	switch backup.Status.Phase {
	case "", kahuapi.BackupPhaseInit:
	default:
		c.logger.WithFields(log.Fields{
			"backup": backup.Name,
			"phase":  backup.Status.Phase,
		}).Infof("Backup: %s is not New, so will not be processed", backup.Name)
		return
	}
	c.genericController.Enqueue(obj)
}

func (c *controller) handleDel(obj interface{}) {
	backup := obj.(*kahuapi.Backup)
	c.deleteBackup(backup.Name, backup)

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
