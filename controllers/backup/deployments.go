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

//func (c *controller) GetDeploymentAndBackup(gvr GroupResouceVersion, name, namespace string,
//	backupClient metaservice.MetaService_BackupClient) error {
//	deployment, err := c.kubeClient.AppsV1().Deployments(namespace).Get(context.TODO(), name, metav1.GetOptions{})
//	if err != nil {
//		return err
//	}
//
//	err = c.backupSend(deployment, deployment.Name, backupClient)
//	if err != nil {
//		return err
//	}
//	return nil
//
//}
//
//func (c *controller) deploymentBackup(gvr GroupResouceVersion, namespace string,
//	backup *PrepareBackup, backupClient metaservice.MetaService_BackupClient) error {
//
//	var labelSelectors map[string]string
//	if backup.Spec.Label != nil {
//		labelSelectors = backup.Spec.Label.MatchLabels
//	}
//
//	selectors := labels.Set(labelSelectors).String()
//	dList, err := c.kubeClient.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{
//		LabelSelector: selectors,
//	})
//	if err != nil {
//		return err
//	}
//
//	for _, deployment := range dList.Items {
//		err = c.GetDeploymentAndBackup(gvr, deployment.Name, deployment.Namespace, backupClient)
//		if err != nil {
//			return err
//		}
//		err = c.GetConfigMapUsedInDeployment(gvr, deployment, backupClient)
//		if err != nil {
//			return err
//		}
//		err = c.GetServiceAccountUsedInDeployment(gvr, deployment, backupClient)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func (c *controller) GetConfigMapUsedInDeployment(gvr GroupResouceVersion, deployment v1.Deployment,
//	backupClient metaservice.MetaService_BackupClient) error {
//	for _, v := range deployment.Spec.Template.Spec.Volumes {
//		if v.ConfigMap != nil {
//			configMap, err := c.GetConfigMap(deployment.Namespace, v.ConfigMap.Name)
//			if err != nil {
//				c.logger.Errorf("unable to get configmap for name: %s", v.ConfigMap.Name)
//				return err
//			}
//
//			c.backupSend(configMap, v.ConfigMap.Name, backupClient)
//		}
//	}
//	return nil
//
//}
//
//func (c *controller) GetServiceAccountUsedInDeployment(gvr GroupResouceVersion, deployment v1.Deployment,
//	backupClient metaservice.MetaService_BackupClient) error {
//	saName := deployment.Spec.Template.Spec.ServiceAccountName
//
//	sa, err := c.GetServiceAccount(deployment.Namespace, saName)
//	if err != nil {
//		c.logger.Errorf("unable to get service account for name: %s", saName)
//	}
//
//	c.backupSend(sa, saName, backupClient)
//
//	return nil
//}
//
//func (c *controller) GetServiceAccount(namespace, name string) (*corev1.ServiceAccount, error) {
//	sa, err := c.kubeClient.CoreV1().ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
//	if err != nil {
//		return nil, err
//	}
//	return sa, err
//}
//
//func (c *controller) GetConfigMap(namespace, name string) (*corev1.ConfigMap, error) {
//	configmap, err := c.kubeClient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
//	if err != nil {
//		return nil, err
//	}
//	return configmap, err
//}
//
//func (c *controller) ListNamespaces(backup *PrepareBackup) ([]string, error) {
//	var namespaceList []string
//	namespaces, err := c.kubeClient.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
//	if err != nil {
//		return namespaceList, err
//	}
//	for _, ns := range namespaces.Items {
//		namespaceList = append(namespaceList, ns.Name)
//
//	}
//	return namespaceList, nil
//}
//
//func (c *controller) getPersistentVolumeClaims(gvr GroupResouceVersion, namespace string, backup *PrepareBackup,
//	backupClient metaservice.MetaService_BackupClient) error {
//
//	c.logger.Infoln("starting collecting persistentvolumeclaims")
//	var labelSelectors map[string]string
//	if backup.Spec.Label != nil {
//		labelSelectors = backup.Spec.Label.MatchLabels
//	}
//
//	selectors := labels.Set(labelSelectors).String()
//	allPVC, err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).List(context.TODO(), metav1.ListOptions{
//		LabelSelector: selectors,
//	})
//	if err != nil {
//		return err
//	}
//
//	c.logger.Infof("the all pvc:%+v", allPVC)
//	for _, pvc := range allPVC.Items {
//		pvcData, err := c.GetPVC(namespace, pvc.Name)
//		if err != nil {
//			return err
//		}
//		err = c.backupSend(pvcData, pvc.Name, backupClient)
//		if err != nil {
//			return err
//		}
//
//	}
//	return nil
//}
//
//func (c *controller) GetPVC(namespace, name string) (*corev1.PersistentVolumeClaim, error) {
//	pvc, err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, metav1.GetOptions{})
//	if err != nil {
//		return nil, err
//	}
//	return pvc, err
//}
//
//func (c *controller) getStorageClass(gvr GroupResouceVersion, backup *PrepareBackup,
//	backupClient metaservice.MetaService_BackupClient) error {
//
//	var labelSelectors map[string]string
//	if backup.Spec.Label != nil {
//		labelSelectors = backup.Spec.Label.MatchLabels
//	}
//
//	selectors := labels.Set(labelSelectors).String()
//	allSC, err := c.kubeClient.StorageV1().StorageClasses().List(context.TODO(), metav1.ListOptions{
//		LabelSelector: selectors,
//	})
//	if err != nil {
//		return err
//	}
//
//	for _, sc := range allSC.Items {
//		scData, err := c.GetSC(sc.Name)
//
//		err = c.backupSend(scData, sc.Name, backupClient)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func (c *controller) GetSC(name string) (*storagev1.StorageClass, error) {
//	sc, err := c.kubeClient.StorageV1().StorageClasses().Get(context.TODO(), name, metav1.GetOptions{})
//	if err != nil {
//		return nil, err
//	}
//	return sc, err
//}
