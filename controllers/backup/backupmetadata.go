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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/soda-cdm/kahu/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	metaservice "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
)

func (ctrl *controller) processMetadataBackup(backup *kahuapi.Backup) error {
	// Validate the Metadatalocation
	backupProvider := backup.Spec.MetadataLocation
	ctrl.logger.Infof("Preparing backup for provider: %s ", backupProvider)
	backuplocation, err := ctrl.backupLocationLister.Get(backupProvider)
	if err != nil {
		ctrl.logger.Errorf("failed to validate backup location, reason: %s", err)
		backup.Status.State = kahuapi.BackupStateFailed
		backup.Status.ValidationErrors = append(backup.Status.ValidationErrors, fmt.Sprintf("%v", err))
		ctrl.updateStatus(backup, ctrl.backupClient, backup.Status)
		return err
	}
	ctrl.logger.Debugf("the provider name in backuplocation:%s", backuplocation)

	ctrl.logger.Infof("Preparing backup request for Provider:%s", backupProvider)
	prepareBackupReq := ctrl.prepareBackupRequest(backup)

	if len(prepareBackupReq.Status.ValidationErrors) > 0 {
		prepareBackupReq.Status.State = kahuapi.BackupStateFailed
		ctrl.updateStatus(prepareBackupReq.Backup, ctrl.backupClient, prepareBackupReq.Status)
		return err
	} else {
		prepareBackupReq.Status.State = kahuapi.BackupStateProcessing
		prepareBackupReq.Status.Stage = kahuapi.BackupStageResources
		prepareBackupReq.Status.ValidationErrors = []string{}
	}
	prepareBackupReq.Status.StartTimestamp = &metav1.Time{Time: time.Now()}
	ctrl.updateStatus(prepareBackupReq.Backup, ctrl.backupClient, prepareBackupReq.Status)

	// Initialize hooks
	err = ctrl.runBackup(prepareBackupReq)
	if err != nil {
		prepareBackupReq.Status.State = kahuapi.BackupStateFailed
	} else {
		prepareBackupReq.Status.State = kahuapi.BackupStateCompleted
		prepareBackupReq.Status.Stage = kahuapi.BackupStageFinished
	}
	prepareBackupReq.Status.LastBackup = &metav1.Time{Time: time.Now()}

	ctrl.logger.Infof("completed backup with status: %s", prepareBackupReq.Status.Stage)
	ctrl.updateStatus(prepareBackupReq.Backup, ctrl.backupClient, prepareBackupReq.Status)
	return err
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

	// validate the namespace from include and exlude list
	for _, err := range utils.ValidateNamespace(backupRequest.Spec.IncludeNamespaces, backupRequest.Spec.ExcludeNamespaces) {
		backupRequest.Status.ValidationErrors = append(backupRequest.Status.ValidationErrors, fmt.Sprintf("Include/Exclude namespace list is not valid: %v", err))
	}

	// till now validation is ok. Set the backupphase as New to start backup
	backupRequest.Status.Stage = kahuapi.BackupStageInitial

	return backupRequest
}

func (ctrl *controller) getResultant(backup *PrepareBackup) []string {
	// validate the resources from include and exlude list
	var includedresourceKindList []string
	var excludedresourceKindList []string

	for _, resource := range backup.Spec.IncludeResources {
		includedresourceKindList = append(includedresourceKindList, resource.Kind)
	}

	if len(includedresourceKindList) == 0 {
		for _, resource := range backup.Spec.ExcludeResources {
			excludedresourceKindList = append(excludedresourceKindList, resource.Kind)
		}
	}

	return utils.GetResultantItems(utils.SupportedResourceList, includedresourceKindList, excludedresourceKindList)

}

func (ctrl *controller) runBackup(backup *PrepareBackup) error {
	ctrl.logger.Infoln("Starting to run backup")
	var backupStatus = []string{}

	metaServiceClient, grpcConn, err := ctrl.fetchMetaServiceClient(backup.Spec.MetadataLocation)
	if err != nil && metaServiceClient == nil {
		ctrl.logger.Errorf("Unable to connect metadata service. %s", err)
		return errors.Wrap(err, fmt.Sprint("Unable to connect metadata service"))
	}
	defer grpcConn.Close()

	backupClient, err := metaServiceClient.Backup(context.Background())
	if err != nil {
		ctrl.logger.Errorf("Unable to get backup client. %s", err)
		return errors.Wrap(err, fmt.Sprint("Unable to get backup client"))
	}
	defer backupClient.CloseAndRecv()

	err = backupClient.Send(&metaservice.BackupRequest{
		Backup: &metaservice.BackupRequest_Identifier{
			Identifier: &metaservice.BackupIdentifier{
				BackupHandle: backup.Name,
			},
		},
	})

	if err != nil {
		ctrl.logger.Errorf("Unable to send data to metadata service %s", err)
		return err
	}

	resultantResource := sets.NewString(ctrl.getResultant(backup)...)

	var allNamespace []string
	if len(backup.Spec.IncludeNamespaces) == 0 {
		allNamespace, _ = ctrl.ListNamespaces(backup)
	}
	resultNs := utils.GetResultantItems(allNamespace, backup.Spec.IncludeNamespaces, backup.Spec.ExcludeNamespaces)

	resultantNamespace := sets.NewString(resultNs...)
	ctrl.logger.Infof("backup will be taken for these resources:%+v", resultantResource)
	ctrl.logger.Infof("backup will be taken for these namespaces:%+v", resultantNamespace)

	for ns, nsVal := range resultantNamespace {
		ctrl.logger.Infof("started backup for namespace:%s", ns)
		for name, val := range resultantResource {
			ctrl.logger.Debug(nsVal, val)
			switch name {
			case utils.Pod:
				err = ctrl.podBackup(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.Deployment:
				err = ctrl.deploymentBackup(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.Configmap:
				err = ctrl.getConfigMapS(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.PVC:
				err = ctrl.getPersistentVolumeClaims(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.SC:
				err = ctrl.getStorageClass(backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.Service:
				err = ctrl.getServices(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.Secret:
				err = ctrl.getSecrets(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.Endpoint:
				err = ctrl.getEndpoints(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.Replicaset:
				err = ctrl.replicaSetBackup(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.Statefulset:
				err = ctrl.getStatefulsets(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			case utils.Daemonset:
				err = ctrl.daemonSetBackup(ns, backup, backupClient)
				if err != nil {
					backup.Status.State = kahuapi.BackupStateFailed
				} else {
					backup.Status.Stage = kahuapi.BackupStageFinished
				}
			default:
				continue
			}
		}
	}

	// add volume backup content in backup
	err = ctrl.backupVolumeBackupContent(backup, backupClient)
	if err != nil {
		backup.Status.State = kahuapi.BackupStateFailed
		ctrl.logger.Errorf("Failed to backup volume backup contents. %s", err)
		return errors.Wrap(err, "unable to backup volume backup contents")
	}

	ctrl.logger.Infof("the intermediate status:%s", backupStatus)

	return err
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
		ctrl.logger.Errorf("Unable to get gvk: %s", err)
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

func (ctrl *controller) deleteMetadataBackup(backup *kahuapi.Backup) error {
	// TODO: delete need to be added
	deleteRequest := &metaservice.DeleteRequest{
		Id: &metaservice.BackupIdentifier{
			BackupHandle: backup.Name,
		},
	}

	metaservice, grpcConn, err := ctrl.fetchMetaServiceClient(backup.Spec.MetadataLocation)
	if err != nil {
		return err
	}
	defer grpcConn.Close()

	_, err = metaservice.Delete(context.Background(), deleteRequest)
	if err != nil {
		ctrl.logger.Errorf("unable to delete metadata backup file %v", backup.Name)
		return fmt.Errorf("unable to delete metadata backup file %v", backup.Name)
	}

	return nil
}
