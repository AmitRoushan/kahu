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

// Code generated by client-gen. DO NOT EDIT.

package v1

import (
	"context"
	"time"

	v1 "github.com/soda-cdm/kahu/apis/kahu/v1"
	scheme "github.com/soda-cdm/kahu/client/clientset/versioned/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// VolumeRestoreContentsGetter has a method to return a VolumeRestoreContentInterface.
// A group's client should implement this interface.
type VolumeRestoreContentsGetter interface {
	VolumeRestoreContents() VolumeRestoreContentInterface
}

// VolumeRestoreContentInterface has methods to work with VolumeRestoreContent resources.
type VolumeRestoreContentInterface interface {
	Create(ctx context.Context, volumeRestoreContent *v1.VolumeRestoreContent, opts metav1.CreateOptions) (*v1.VolumeRestoreContent, error)
	UpdateStatus(ctx context.Context, volumeRestoreContent *v1.VolumeRestoreContent, opts metav1.UpdateOptions) (*v1.VolumeRestoreContent, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.VolumeRestoreContent, error)
	List(ctx context.Context, opts metav1.ListOptions) (*v1.VolumeRestoreContentList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.VolumeRestoreContent, err error)
	VolumeRestoreContentExpansion
}

// volumeRestoreContents implements VolumeRestoreContentInterface
type volumeRestoreContents struct {
	client rest.Interface
}

// newVolumeRestoreContents returns a VolumeRestoreContents
func newVolumeRestoreContents(c *KahuV1Client) *volumeRestoreContents {
	return &volumeRestoreContents{
		client: c.RESTClient(),
	}
}

// Get takes name of the volumeRestoreContent, and returns the corresponding volumeRestoreContent object, and an error if there is any.
func (c *volumeRestoreContents) Get(ctx context.Context, name string, options metav1.GetOptions) (result *v1.VolumeRestoreContent, err error) {
	result = &v1.VolumeRestoreContent{}
	err = c.client.Get().
		Resource("volumerestorecontents").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of VolumeRestoreContents that match those selectors.
func (c *volumeRestoreContents) List(ctx context.Context, opts metav1.ListOptions) (result *v1.VolumeRestoreContentList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1.VolumeRestoreContentList{}
	err = c.client.Get().
		Resource("volumerestorecontents").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested volumeRestoreContents.
func (c *volumeRestoreContents) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Resource("volumerestorecontents").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a volumeRestoreContent and creates it.  Returns the server's representation of the volumeRestoreContent, and an error, if there is any.
func (c *volumeRestoreContents) Create(ctx context.Context, volumeRestoreContent *v1.VolumeRestoreContent, opts metav1.CreateOptions) (result *v1.VolumeRestoreContent, err error) {
	result = &v1.VolumeRestoreContent{}
	err = c.client.Post().
		Resource("volumerestorecontents").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(volumeRestoreContent).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *volumeRestoreContents) UpdateStatus(ctx context.Context, volumeRestoreContent *v1.VolumeRestoreContent, opts metav1.UpdateOptions) (result *v1.VolumeRestoreContent, err error) {
	result = &v1.VolumeRestoreContent{}
	err = c.client.Put().
		Resource("volumerestorecontents").
		Name(volumeRestoreContent.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(volumeRestoreContent).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the volumeRestoreContent and deletes it. Returns an error if one occurs.
func (c *volumeRestoreContents) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return c.client.Delete().
		Resource("volumerestorecontents").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *volumeRestoreContents) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Resource("volumerestorecontents").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched volumeRestoreContent.
func (c *volumeRestoreContents) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.VolumeRestoreContent, err error) {
	result = &v1.VolumeRestoreContent{}
	err = c.client.Patch(pt).
		Resource("volumerestorecontents").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
