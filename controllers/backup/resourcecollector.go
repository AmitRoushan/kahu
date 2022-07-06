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

	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (c *controller) getServices(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	allServices, err := c.kubeClient.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, service := range allServices.Items {
		serviceData, err := c.kubeClient.CoreV1().Services(namespace).Get(context.TODO(), service.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		err = c.backupSend(serviceData, serviceData.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *controller) getConfigMapS(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	configList, err := c.kubeClient.CoreV1().ConfigMaps(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, configMap := range configList.Items {
		config_data, err := c.GetConfigMap(namespace, configMap.Name)

		err = c.backupSend(config_data, configMap.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *controller) getSecrets(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	secretList, err := c.kubeClient.CoreV1().Secrets(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, secret := range secretList.Items {
		secretData, err := c.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), secret.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		err = c.backupSend(secretData, secret.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *controller) getEndpoints(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	endpointList, err := c.kubeClient.CoreV1().Endpoints(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, endpoint := range endpointList.Items {
		endpointData, err := c.kubeClient.CoreV1().Endpoints(namespace).Get(context.TODO(), endpoint.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		err = c.backupSend(endpointData, endpointData.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *controller) getReplicasets(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	replicasetList, err := c.kubeClient.AppsV1().ReplicaSets(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, replicaset := range replicasetList.Items {
		replicasetData, err := c.kubeClient.AppsV1().ReplicaSets(namespace).Get(context.TODO(), replicaset.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		err = c.backupSend(replicasetData, replicaset.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *controller) getStatefulsets(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
	backupClient metaservice.MetaService_BackupClient) error {

	var labelSelectors map[string]string
	if backup.Spec.Label != nil {
		labelSelectors = backup.Spec.Label.MatchLabels
	}

	selectors := labels.Set(labelSelectors).String()
	statefulList, err := c.kubeClient.AppsV1().StatefulSets(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selectors,
	})
	if err != nil {
		return err
	}

	for _, stateful := range statefulList.Items {
		statefulData, err := c.kubeClient.AppsV1().StatefulSets(namespace).Get(context.TODO(), stateful.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		err = c.backupSend(statefulData, stateful.Name, backupClient)
		if err != nil {
			return err
		}

	}
	return nil
}
