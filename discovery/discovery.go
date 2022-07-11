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

package discovery

import (
	"k8s.io/client-go/tools/cache"
	"reflect"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
)

const (
	namespaceScopeIndex = "api-resource-namespace-scope-index"
	clusterScopeIndex   = "api-resource-cluster-scope-index"
)

// DiscoveryHelper exposes functions for Kubernetes discovery API.
type DiscoveryHelper interface {
	// APIGroups returns current supported APIGroups in kubernetes cluster
	APIGroups() []metav1.APIGroup

	// ServerVersion returns kubernetes version information
	ServerVersion() *version.Info

	// Resources returns the current set of resources retrieved from discovery
	Resources() []*metav1.APIResourceList

	// ByGroupVersionKind gets a fully-resolved GroupVersionResource and an
	// APIResource for the provided GroupVersionKind.
	ByGroupVersionKind(input schema.GroupVersionKind) (schema.GroupVersionResource,
		metav1.APIResource,
		error)

	// Refresh updates API resource list with discovery helper
	Refresh() error

	NamespaceScopedResources() ([]*metav1.APIResource, error)

	ClusterScopedResources() ([]*metav1.APIResource, error)
}

type discoverHelper struct {
	discoveryClient discovery.DiscoveryInterface
	logger          log.FieldLogger
	lock            sync.RWMutex
	resources       []*metav1.APIResourceList
	//byGroupVersionKind map[schema.GroupVersionKind]metav1.APIResource
	k8sAPIGroups        []metav1.APIGroup
	k8sVersion          *version.Info
	apiIndexedResources cache.Indexer
}

var _ DiscoveryHelper = &discoverHelper{}

func NewDiscoveryHelper(discoveryClient discovery.DiscoveryInterface,
	logger log.FieldLogger) (DiscoveryHelper, error) {
	helper := &discoverHelper{
		discoveryClient:     discoveryClient,
		logger:              logger,
		apiIndexedResources: cache.NewIndexer(gvkKeyFunc, newApiResourcesIndexers()),
	}

	if err := helper.Refresh(); err != nil {
		return nil, err
	}
	return helper, nil
}

func getAPIResource(obj interface{}) (*metav1.APIResource, error) {
	switch t := obj.(type) {
	case metav1.APIResource:
		return &t, nil
	case *metav1.APIResource:
		return t, nil
	default:
		return nil, errors.Errorf("invalid api resource format %s", reflect.TypeOf(t))
	}
}

func gvkKeyFunc(obj interface{}) (string, error) {
	resource, err := getAPIResource(obj)
	if err != nil {
		return "", err
	}

	return schema.GroupVersionKind{
		Group:   resource.Group,
		Version: resource.Version,
		Kind:    resource.Kind,
	}.String(), nil
}

func newApiResourcesIndexers() cache.Indexers {
	return cache.Indexers{
		namespaceScopeIndex: func(obj interface{}) ([]string, error) {
			resource, err := getAPIResource(obj)
			if err != nil {
				log.Warningf("invalid api resource %s", reflect.TypeOf(obj))
				return []string{}, nil
			}
			if resource.Namespaced {
				return []string{schema.GroupVersionKind{
					Group:   resource.Group,
					Version: resource.Version,
					Kind:    resource.Kind,
				}.String()}, nil
			}
			return []string{}, nil
		},
		clusterScopeIndex: func(obj interface{}) ([]string, error) {
			resource, err := getAPIResource(obj)
			if err != nil {
				log.Warningf("invalid api resource %s", reflect.TypeOf(obj))
				return []string{}, nil
			}
			if !resource.Namespaced {
				return []string{schema.GroupVersionKind{
					Group:   resource.Group,
					Version: resource.Version,
					Kind:    resource.Kind,
				}.String()}, nil
			}
			return []string{}, nil
		},
	}
}

func (helper *discoverHelper) ByGroupVersionKind(
	input schema.GroupVersionKind) (schema.GroupVersionResource,
	metav1.APIResource, error) {
	helper.lock.RLock()
	defer helper.lock.RUnlock()

	if obj, exist, err := helper.apiIndexedResources.GetByKey(input.String()); err != nil && exist {
		resource, err := getAPIResource(obj)
		if err != nil {
			log.Warningf("invalid api resource %s", reflect.TypeOf(obj))
			return schema.GroupVersionResource{}, metav1.APIResource{}, err
		}
		return schema.GroupVersionResource{
			Group:    input.Group,
			Version:  input.Version,
			Resource: resource.Name,
		}, *resource, nil
	}

	err := helper.Refresh()
	if err != nil {
		return schema.GroupVersionResource{}, metav1.APIResource{}, err
	}

	if obj, exist, err := helper.apiIndexedResources.GetByKey(input.String()); err != nil && exist {
		resource, err := getAPIResource(obj)
		if err != nil {
			log.Warningf("invalid api resource %s", reflect.TypeOf(obj))
			return schema.GroupVersionResource{}, metav1.APIResource{}, err
		}
		return schema.GroupVersionResource{
			Group:    input.Group,
			Version:  input.Version,
			Resource: resource.Name,
		}, *resource, nil
	}

	return schema.GroupVersionResource{},
		metav1.APIResource{},
		errors.Errorf("APIResource not found for GroupVersionKind %v ", input)
}

func (helper *discoverHelper) Refresh() error {
	helper.lock.Lock()
	defer helper.lock.Unlock()

	apiResourcesList, err := helper.discoveryClient.ServerPreferredResources()
	if err != nil {
		if discoveryErr, ok := err.(*discovery.ErrGroupDiscoveryFailed); ok {
			for groupVersion, err := range discoveryErr.Groups {
				helper.logger.WithError(err).Warnf("Failed to discover group: %v", groupVersion)
			}
		}
		return err
	}

	helper.resources = discovery.FilteredBy(
		discovery.ResourcePredicateFunc(filterByVerbs),
		apiResourcesList,
	)

	//helper.byGroupVersionKind = make(map[schema.GroupVersionKind]metav1.APIResource)
	for _, resourceGroup := range helper.resources {
		gv, err := schema.ParseGroupVersion(resourceGroup.GroupVersion)
		if err != nil {
			return errors.Wrapf(err, "unable to parse GroupVersion %s", resourceGroup.GroupVersion)
		}

		for _, resource := range resourceGroup.APIResources {
			gvk := gv.WithKind(resource.Kind)
			resource.Group = gvk.Group
			resource.Version = gvk.Version
			// helper.byGroupVersionKind[gvk] = resource
			err := helper.apiIndexedResources.Add(&resource)
			if err != nil {
				log.Warningf("Failed to add gvk %s", gvk)
			}
		}
	}

	apiGroupList, err := helper.discoveryClient.ServerGroups()
	if err != nil {
		return errors.WithStack(err)
	}
	helper.k8sAPIGroups = apiGroupList.Groups

	serverVersion, err := helper.discoveryClient.ServerVersion()
	if err != nil {
		return errors.WithStack(err)
	}

	helper.k8sVersion = serverVersion

	for _, value := range helper.apiIndexedResources.List() {
		helper.logger.Infof("---> value %+v", value)
	}

	return nil
}

func (helper *discoverHelper) Resources() []*metav1.APIResourceList {
	helper.lock.RLock()
	defer helper.lock.RUnlock()
	return helper.resources
}

func (helper *discoverHelper) NamespaceScopedResources() ([]*metav1.APIResource, error) {
	gvks := helper.apiIndexedResources.ListIndexFuncValues(namespaceScopeIndex)

	apiResources := make([]*metav1.APIResource, 0)
	for _, gvk := range gvks {
		obj, err := helper.apiIndexedResources.ByIndex(namespaceScopeIndex, gvk)
		if err != nil {
			log.Errorf("unable to get namespace scoped index %s", gvk)
			return nil, errors.Wrap(err, "unable to get namespace scoped index")
		}
		resource, err := getAPIResource(obj)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get namespace scoped index")
		}
		apiResources = append(apiResources, resource)
	}

	return apiResources, nil
}

func (helper *discoverHelper) ClusterScopedResources() ([]*metav1.APIResource, error) {
	gvks := helper.apiIndexedResources.ListIndexFuncValues(clusterScopeIndex)

	apiResources := make([]*metav1.APIResource, 0)
	for _, gvk := range gvks {
		obj, err := helper.apiIndexedResources.ByIndex(clusterScopeIndex, gvk)
		if err != nil {
			log.Errorf("unable to get namespace scoped index %s", gvk)
			return nil, errors.Wrap(err, "unable to get cluster scoped index")
		}
		resource, err := getAPIResource(obj)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get cluster scoped index")
		}
		apiResources = append(apiResources, resource)
	}

	return apiResources, nil
}

func (helper *discoverHelper) APIGroups() []metav1.APIGroup {
	helper.lock.RLock()
	defer helper.lock.RUnlock()
	return helper.k8sAPIGroups
}

func (helper *discoverHelper) ServerVersion() *version.Info {
	helper.lock.RLock()
	defer helper.lock.RUnlock()
	return helper.k8sVersion
}

func filterByVerbs(groupVersion string, r *metav1.APIResource) bool {
	return discovery.SupportsAllVerbs{Verbs: []string{"list", "create", "get", "delete"}}.Match(groupVersion, r)
}
