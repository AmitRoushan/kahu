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
	"github.com/soda-cdm/kahu/discovery"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"reflect"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
)

var excludeResourceType = [...]string{
	NodeKind,
	EventKind,
}

type Context interface {
	Complete() error
	IsComplete() bool
	GetNamespaces() []string
	GetKindResources(kind string) []*unstructured.Unstructured
	GetResources() []*unstructured.Unstructured
	GetClusterScopeResources() []*unstructured.Unstructured
	SyncResources(*kahuapi.Backup) (*kahuapi.Backup, error)
}

type backupContext struct {
	logger                log.FieldLogger
	backup                *kahuapi.Backup
	dynamicClient         dynamic.Interface
	kubeClient            kubernetes.Interface
	isBackupCacheComplete bool
	backupIndexCache      cache.Indexer
	discoveryHelper       discovery.DiscoveryHelper
	controller            *controller
}

func newContext(backup *kahuapi.Backup, ctrl *controller) Context {
	logger := ctrl.logger.WithField("backup", backup.Name)
	return &backupContext{
		logger:        logger,
		backup:        backup.DeepCopy(),
		dynamicClient: ctrl.dynamicClient,
		kubeClient:    ctrl.kubeClient,
		backupIndexCache: cache.NewIndexer(cache.MetaNamespaceKeyFunc,
			newBackupObjectIndexers(logger)),
		isBackupCacheComplete: false,
		discoveryHelper:       ctrl.discoveryHelper,
		controller:            ctrl,
	}
}

func unstructuredResourceKeyFunc(resource *unstructured.Unstructured) string {
	if resource.GetNamespace() == "" {
		return fmt.Sprintf("%s.%s/%s", resource.GetKind(),
			resource.GetAPIVersion(),
			resource.GetName())
	}
	return fmt.Sprintf("%s.%s/%s/%s", resource.GetKind(),
		resource.GetAPIVersion(),
		resource.GetNamespace(),
		resource.GetName())
}

func newBackupObjectIndexers(logger log.FieldLogger) cache.Indexers {
	return cache.Indexers{
		backupCacheNamespaceIndex: func(obj interface{}) ([]string, error) {
			keys := make([]string, 0)
			var namespaceResource *unstructured.Unstructured
			switch t := obj.(type) {
			case *unstructured.Unstructured:
				namespaceResource = t
			case unstructured.Unstructured:
				namespaceResource = t.DeepCopy()
			default:
				return keys, nil
			}

			if namespaceResource.GetKind() == "Namespace" {
				keys = append(keys, namespaceResource.GetName())
			}

			return keys, nil
		},
		backupCacheResourceIndex: func(obj interface{}) ([]string, error) {
			keys := make([]string, 0)
			var resource *unstructured.Unstructured
			switch t := obj.(type) {
			case *unstructured.Unstructured:
				resource = t
			case unstructured.Unstructured:
				resource = t.DeepCopy()
			default:
				return keys, nil
			}

			return append(keys, resource.GetKind()), nil
		},
		backupCacheObjectClusterResourceIndex: func(obj interface{}) ([]string, error) {
			keys := make([]string, 0)
			var resource *unstructured.Unstructured
			switch t := obj.(type) {
			case *unstructured.Unstructured:
				resource = t
			case unstructured.Unstructured:
				resource = t.DeepCopy()
			default:
				return keys, nil
			}

			if resource.GetNamespace() == "" {
				keys = append(keys, resource.GetName())
			}

			return keys, nil
		},
	}
}

func (ctx *backupContext) Complete() error {
	if ctx.isBackupCacheComplete {
		return nil
	}

	// if phase has not completed validation, ignore populating backup objects
	if ctx.backup.Status.Phase == kahuapi.BackupPhaseInit {
		return errors.New("backup has not finished validation")
	}

	// populate all backup resources in cache
	err := ctx.populateCacheFromBackupSpec()
	if err != nil {
		return err
	}

	if err == nil {
		ctx.isBackupCacheComplete = true
	}

	return err
}

func (ctx *backupContext) SyncResources(backup *kahuapi.Backup) (*kahuapi.Backup, error) {
	resources := ctx.GetResources()
	backupResources := make([]kahuapi.BackupResource, 0)

	for _, resource := range resources {
		backupResources = append(backupResources, kahuapi.BackupResource{
			TypeMeta: metav1.TypeMeta{
				APIVersion: resource.GetAPIVersion(),
				Kind:       resource.GetKind(),
			},
			ResourceName: resource.GetName(),
			Namespace:    resource.GetNamespace(),
		})
	}

	backup, err := ctx.controller.updateBackupStatus(ctx.backup, kahuapi.BackupStatus{
		Resources: backupResources,
	})
	if err != nil {
		return backup, err
	}
	ctx.backup = backup

	return backup, nil
}

func (ctx *backupContext) GetClusterScopeResources() []*unstructured.Unstructured {
	resources := make([]*unstructured.Unstructured, 0)
	indexNames := ctx.backupIndexCache.ListIndexFuncValues(backupCacheObjectClusterResourceIndex)
	for _, indexName := range indexNames {
		indexResources, err := ctx.backupIndexCache.ByIndex(backupCacheObjectClusterResourceIndex, indexName)
		if err != nil {
			ctx.logger.Errorf("Unable to get cluster scope resources")
			break
		}
		for _, indexResource := range indexResources {
			switch resource := indexResource.(type) {
			case unstructured.Unstructured:
				resources = append(resources, resource.DeepCopy())
			case *unstructured.Unstructured:
				resources = append(resources, resource)
			default:
				ctx.logger.Warningf("invalid resource format "+
					"in backup cache. %s", reflect.TypeOf(resource))
			}
		}
	}

	return resources
}

func (ctx *backupContext) IsComplete() bool {
	return ctx.isBackupCacheComplete
}

func (ctx *backupContext) GetNamespaces() []string {
	return ctx.getCachedNamespaceNames()
}

func (ctx *backupContext) GetResources() []*unstructured.Unstructured {
	resources := make([]*unstructured.Unstructured, 0)
	resourceList := ctx.backupIndexCache.List()
	for _, t := range resourceList {
		switch resource := t.(type) {
		case unstructured.Unstructured:
			resources = append(resources, resource.DeepCopy())
		case *unstructured.Unstructured:
			resources = append(resources, resource)
		default:
			ctx.logger.Warningf("invalid resource format "+
				"in backup cache. %s", reflect.TypeOf(resource))
		}
	}

	return resources
}

func (ctx *backupContext) GetKindResources(kind string) []*unstructured.Unstructured {
	resources := make([]*unstructured.Unstructured, 0)

	indexResources, err := ctx.backupIndexCache.ByIndex(backupCacheResourceIndex, kind)
	if err != nil {
		ctx.logger.Errorf("Unable to get cluster scope resources")
		return resources
	}
	for _, indexResource := range indexResources {
		switch resource := indexResource.(type) {
		case unstructured.Unstructured:
			resources = append(resources, resource.DeepCopy())
		case *unstructured.Unstructured:
			resources = append(resources, resource)
		default:
			ctx.logger.Warningf("invalid resource format "+
				"in backup cache. %s", reflect.TypeOf(resource))
		}
	}

	return resources
}

func (ctx *backupContext) populateCacheFromBackupSpec() error {
	// collect namespaces
	err := ctx.collectNamespacesWithBackupSpec()
	if err != nil {
		return err
	}

	// collect namespaces
	namespaces := ctx.getCachedNamespaceNames()
	ctx.logger.Infof("Selected namespaces for backup %s", strings.Join(namespaces, ", "))

	// retrieve kubernetes cluster API resources
	apiResources, err := ctx.discoveryHelper.GetNamespaceScopedAPIResources()
	if err != nil {
		return errors.Wrap(err, "unable to get namespace scoped resources")
	}

	apiResources = ctx.getFilteredNamespacedResources(apiResources)

	for _, namespace := range namespaces {
		ctx.logger.Infof("Collecting resources for namespace %s", namespace)
		resources, err := ctx.collectNamespaceResources(namespace, apiResources)
		if err != nil {
			ctx.logger.Errorf("unable to retrieve resource for namespace %s", namespace)
			return errors.Wrap(err,
				fmt.Sprintf("unable to retrieve resource for namespace %s", namespace))
		}

		ctx.logger.Infof("Collected resources for namespace %s", namespace)
		for _, resource := range resources {
			ctx.logger.Infof("Resource %s", unstructuredResourceKeyFunc(resource))
			err := ctx.backupIndexCache.Add(resource.DeepCopy())
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("unable to populate backup "+
					"cache for resource %s", resource.GetName()))
			}
		}
	}

	return nil
}

func (ctx *backupContext) collectNamespacesWithBackupSpec() error {
	// collect namespaces
	namespaces, err := ctx.kubeClient.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		ctx.logger.Errorf("Unable to list namespace. %s", err)
		return errors.Wrap(err, "unable to get namespaces")
	}

	namespaceList := make(map[string]v1.Namespace, 0)
	includeList := sets.NewString(ctx.backup.Spec.IncludeNamespaces...)
	excludeList := sets.NewString(ctx.backup.Spec.ExcludeNamespaces...)

	for _, namespace := range namespaces.Items {
		if excludeList.Has(namespace.Name) {
			continue
		}
		if includeList.Len() > 0 &&
			!includeList.Has(namespace.Name) {
			continue
		}
		namespaceList[namespace.Name] = namespace
	}

	for name, namespace := range namespaceList {
		namespace.Kind = "Namespace"
		namespace.APIVersion = "v1"
		unMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(namespace.DeepCopy())
		if err != nil {
			ctx.logger.Warningf("Failed to translate namespace(%s) to "+
				"unstructured. %s", name, err)
		}
		err = ctx.backupIndexCache.Add(&unstructured.Unstructured{
			Object: unMap,
		})
		if err != nil {
			ctx.logger.Warningf("Failed to add namespace(%s). %s", name, err)
		}
	}

	return nil
}

func (ctx *backupContext) getCachedNamespaceNames() []string {
	// collect namespace names
	return ctx.backupIndexCache.ListIndexFuncValues(backupCacheNamespaceIndex)
}

func (ctx *backupContext) getFilteredNamespacedResources(
	apiResources []*metav1.APIResource) []*metav1.APIResource {
	// TODO (Amit Roushan): Need to add filter for supported resources
	return apiResources
}

func (ctx *backupContext) applyResourceFilters(
	backup *kahuapi.Backup,
	resources []unstructured.Unstructured) []*unstructured.Unstructured {
	filteredResources := make([]*unstructured.Unstructured, 0)
	for _, resource := range resources {
		filterResource := resource.DeepCopy()
		if ctx.isResourceNeedBackup(backup, filterResource) {
			filteredResources = append(filteredResources, filterResource)
		}
	}

	return filteredResources
}

func (ctx *backupContext) isResourceNeedBackup(
	backup *kahuapi.Backup,
	resource *unstructured.Unstructured) bool {
	// evaluate exclude resources
	for _, spec := range backup.Spec.ExcludeResources {
		resourceKind := resource.GetKind()
		resourceName := resource.GetName()
		if spec.Kind == resourceKind && spec.IsRegex {
			regex, err := regexp.Compile(spec.Name)
			if err != nil {
				ctx.logger.Warningf("Unable to compile regex %s", spec.Name)
				continue
			}
			return !regex.Match([]byte(resourceName))
		} else if spec.Kind == resourceKind {
			if resourceName == "" || resourceName == spec.Name {
				return false
			}
		}
	}

	// evaluate include resources
	for _, spec := range backup.Spec.IncludeResources {
		resourceKind := resource.GetKind()
		resourceName := resource.GetName()
		if spec.Kind == resourceKind && spec.IsRegex {
			regex, err := regexp.Compile(spec.Name)
			if err != nil {
				ctx.logger.Warningf("Unable to compile regex %s", spec.Name)
				continue
			}
			return regex.Match([]byte(resourceName))
		} else if spec.Kind == resourceKind {
			if resourceName == "" || resourceName == spec.Name {
				return true
			}
		}

		return false
	}

	return true
}

func (ctx *backupContext) collectNamespaceResources(
	namespace string,
	apiResources []*metav1.APIResource) ([]*unstructured.Unstructured, error) {

	resources := make([]*unstructured.Unstructured, 0)
	for _, resource := range apiResources {
		gvr := schema.GroupVersionResource{
			Group:    resource.Group,
			Version:  resource.Version,
			Resource: resource.Name,
		}
		unstructuredList, err := ctx.dynamicClient.
			Resource(gvr).
			Namespace(namespace).
			List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, errors.Wrap(err, fmt.Sprintf("failed to list resource "+
				"info for %s", gvr))
		}

		unstructuredResources := make([]unstructured.Unstructured, 0)
		for _, item := range unstructuredList.Items {
			item.SetAPIVersion(schema.GroupVersion{
				Group:   resource.Group,
				Version: resource.Version,
			}.String())
			item.SetKind(resource.Kind)
			unstructuredResources = append(unstructuredResources, item)
		}

		filteredResources := ctx.applyResourceFilters(ctx.backup, unstructuredResources)

		// add in final list
		resources = append(resources, filteredResources...)
	}

	return resources, nil
}
