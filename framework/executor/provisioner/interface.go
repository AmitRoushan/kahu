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

package provisioner

import (
	"github.com/soda-cdm/kahu/utils/k8sresource"
	corev1 "k8s.io/api/core/v1"
)

type Interface interface {
	Start(workloadIndex string, namespace string, podTemplate *corev1.PodTemplateSpec) error
	Stop(workloadIndex string, namespace string) error
	AddService(workloadIndex string, namespace string, servicePorts []corev1.ServicePort) (k8sresource.ResourceReference, error)
	Remove(workloadIndex string, namespace string) error
}

type Factory interface {
	Deployment() Interface
}
