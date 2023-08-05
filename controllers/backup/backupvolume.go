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
	"fmt"

	"github.com/pkg/errors"
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/framework/executor/resourcebackup"
	"github.com/soda-cdm/kahu/utils/k8sresource"
	"github.com/soda-cdm/kahu/volume/group"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (ctrl *controller) processVolumeBackup(backup *kahuapi.Backup,
	pvcResources []k8sresource.Resource,
	bl resourcebackup.Interface) (*kahuapi.Backup, error) {
	ctrl.logger.Infof("Processing Volume backup(%s)", backup.Name)
	if len(pvcResources) == 0 {
		// set annotation for
		return backup, nil
	}

	//get all snapshot under the backup
	snapshots, err := ctrl.volumeHandler.Snapshot().GetSnapshotsByBackup(backup.Name)
	if err != nil {
		return backup, err
	}

	backup, snapshotVolumes, err := ctrl.backupSnapshotVolumes(backup, snapshots, bl)
	if err != nil {
		return backup, err
	}

	// filter snapshot volumes and backup rest of volumes
	if len(snapshotVolumes) > 0 {
		filteredPV := make([]k8sresource.Resource, 0)
		for _, pvcResource := range pvcResources {
			if has(snapshotVolumes, pvcResource) {
				continue
			}
			filteredPV = append(filteredPV, pvcResource)
		}
		pvcResources = filteredPV
	}

	pvcs, err := ctrl.getVolumes(backup, pvcResources)
	if err != nil {
		return backup, err
	}

	return ctrl.backupVolumes(backup, pvcs, bl)
}

func has(snapshotVols []kahuapi.ResourceReference, resource k8sresource.Resource) bool {
	for _, vol := range snapshotVols {
		if vol.Name == resource.GetName() && vol.Namespace == resource.GetNamespace() {
			return true
		}
	}

	return false
}

func (ctrl *controller) backupSnapshotVolumes(backup *kahuapi.Backup,
	snapshots []kahuapi.VolumeSnapshot,
	bl resourcebackup.Interface) (*kahuapi.Backup, []kahuapi.ResourceReference, error) {
	ctrl.logger.Infof("Backup snapshot volume for backup[%s]", backup.Name)
	if len(snapshots) == 0 {
		return backup, nil, nil
	}

	// create groups based on snapshot provisioners
	snapshotGroups, err := ctrl.volumeHandler.Group().BySnapshot(snapshots, group.WithProvisioner())
	if err != nil {
		return backup, nil, err
	}

	var blName string
	volumes := make([]kahuapi.ResourceReference, 0)
	for _, snapshotGroup := range snapshotGroups {
		backup, blName, err = ctrl.decideVolumeBackupLocation(backup, snapshotGroup.GetProvisionerName())
		if err != nil {
			return backup, volumes, err
		}
		if blName == "" {
			return backup, volumes, errors.Errorf("not able to decide backup location for %s", snapshotGroup.GetProvisionerName())
		}

		vbc, err := ctrl.volumeHandler.Backup().BySnapshots(backup, snapshotGroup, blName)
		if err != nil {
			return backup, volumes, err
		}

		// backup PVC meta
		err = bl.UploadK8SResource(ctrl.ctx, backup.Name, snapshotGroup.GetResources())
		if err != nil {
			return nil, volumes, err
		}

		// backup vbc
		vbcResource, err := k8sresource.ToResource(vbc)
		if err != nil {
			return backup, volumes, err
		}
		err = bl.UploadK8SResource(ctrl.ctx, backup.Name, []k8sresource.Resource{
			vbcResource,
		})
		if err != nil {
			return nil, volumes, err
		}
	}

	for _, snapshot := range snapshots {
		volumes = append(volumes, snapshot.Spec.List...)
	}

	return backup, volumes, nil
}

func (ctrl *controller) backupVolumes(backup *kahuapi.Backup,
	volumes []*corev1.PersistentVolumeClaim,
	bl resourcebackup.Interface) (*kahuapi.Backup, error) {
	ctrl.logger.Infof("Volume backup (%s) started", backup.Name)
	if len(volumes) == 0 {
		return backup, nil
	}

	// create groups based on snapshot and non snapshot volumes
	volumeGroups, err := ctrl.volumeHandler.Group().ByPVC(volumes, group.WithProvisioner())
	if err != nil {
		return backup, err
	}

	var blName string
	for _, volumeGroup := range volumeGroups {
		backup, blName, err = ctrl.decideVolumeBackupLocation(backup, volumeGroup.GetProvisionerName())
		if err != nil {
			return backup, err
		}

		vbc, err := ctrl.volumeHandler.Backup().ByPVC(backup.Name, volumeGroup, blName)
		if err != nil {
			return backup, err
		}

		// backup PV meta
		err = bl.UploadK8SResource(ctrl.ctx, backup.Name, volumeGroup.GetResources())
		if err != nil {
			return nil, err
		}

		// backup vbc
		vbcResource, err := k8sresource.ToResource(vbc)
		if err != nil {
			return backup, err
		}
		err = bl.UploadK8SResource(ctrl.ctx, backup.Name, []k8sresource.Resource{
			vbcResource,
		})
		if err != nil {
			return nil, err
		}
	}

	return backup, nil
}

func (ctrl *controller) decideVolumeBackupLocation(backup *kahuapi.Backup,
	volumeProvisioner string) (*kahuapi.Backup, string, error) {
	// get volume backup provider for volume provisioner
	providerName, err := ctrl.volumeHandler.Provider().GetNameByProvisioner(volumeProvisioner)
	if err != nil {
		ctrl.logger.Errorf("Unable to get provider.%s", err)
		if _, err = ctrl.updateBackupStatusWithEvent(backup, kahuapi.BackupStatus{
			State: kahuapi.BackupStateFailed,
		}, corev1.EventTypeWarning, EventVolumeBackupFailed,
			fmt.Sprintf("Volume backup provider not available for provisioner[%s]. %s",
				volumeProvisioner, err)); err != nil {
			return backup, "", errors.Wrap(err, "Volume backup failed")
		}
		return backup, "", errors.Wrap(err, "Failed to ensure volume backup location")
	}

	// check if any volume backup location available in backup
	for _, volBaclupLocation := range backup.Spec.VolumeBackupLocations {
		bl, err := ctrl.blLister.Get(volBaclupLocation)
		if err != nil && apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return backup, "", err
		}
		if bl.Spec.ProviderName == providerName {
			return backup, bl.Name, nil
		}
	}

	// try to assign default backup location
	blName, err := ctrl.volumeHandler.Location().GetDefaultLocation(providerName)
	if err != nil {
		ctrl.logger.Errorf("Unable to get default provider.%s", err)
		if _, err = ctrl.updateBackupStatusWithEvent(backup, kahuapi.BackupStatus{
			State: kahuapi.BackupStateFailed,
		}, corev1.EventTypeWarning, EventVolumeBackupFailed,
			fmt.Sprintf("Default volume backup provider not available for provisioner[%s]. %s",
				providerName, err)); err != nil {
			return backup, "", errors.Wrap(err, "Volume backup failed")
		}
		return backup, "", errors.Wrap(err, "Failed to ensure volume backup location")
	}

	return backup, blName, nil
}

//func (ctrl *controller) ensureVolumeBackupParameters(backup *kahuapi.Backup) (map[string]string, []string) {
//	validationErrors := make([]string, 0)
//	volBackupParam := make(map[string]string, 0)
//
//	backupSpec := backup.Spec
//
//	if len(backupSpec.VolumeBackupLocations) == 0 {
//		validationErrors = append(validationErrors, "volume backup location can not be empty "+
//			"for backups with volumes")
//		return volBackupParam, validationErrors
//	}
//
//	for _, volumeBackupLocation := range backupSpec.VolumeBackupLocations {
//		volBackupLoc, err := ctrl.backupLocationLister.Get(volumeBackupLocation)
//		if apierrors.IsNotFound(err) {
//			validationErrors = append(validationErrors,
//				fmt.Sprintf("volume backup location(%s) not found", volumeBackupLocation))
//			return volBackupParam, validationErrors
//		}
//
//		if err == nil && volBackupLoc != nil {
//			_, err := ctrl.providerLister.Get(volBackupLoc.Spec.ProviderName)
//			if apierrors.IsNotFound(err) {
//				validationErrors = append(validationErrors,
//					fmt.Sprintf("invalid provider name (%s) configured for volume backup location (%s)",
//						volBackupLoc.Spec.ProviderName, volumeBackupLocation))
//				return volBackupParam, validationErrors
//			}
//		}
//
//		// TODO: Add Provider configurations
//		return volBackupLoc.Spec.Config, nil
//	}
//
//	return volBackupParam, validationErrors
//}
//
//func (ctrl *controller) removeVolumeBackup(
//	backup *kahuapi.Backup) error {
//
//	vbcList, err := ctrl.volumeBackupClient.List(context.TODO(), metav1.ListOptions{
//		LabelSelector: labels.Set{
//			volumeContentBackupLabel: backup.Name,
//		}.String(),
//	})
//	if err != nil {
//		ctrl.logger.Errorf("Unable to get volume backup content list %s", err)
//		return errors.Wrap(err, "unable to get volume backup content list")
//	}
//
//	for _, vbc := range vbcList.Items {
//		if vbc.DeletionTimestamp != nil { // ignore deleting volume backup content
//			continue
//		}
//		err := ctrl.volumeBackupClient.Delete(context.TODO(), vbc.Name, metav1.DeleteOptions{})
//		if err != nil {
//			ctrl.logger.Errorf("Failed to delete volume backup content %s", err)
//			return errors.Wrap(err, "Unable to delete volume backup content")
//		}
//	}
//
//	return nil
//}

func (ctrl *controller) getVolumes(
	backup *kahuapi.Backup,
	resources []k8sresource.Resource) ([]*corev1.PersistentVolumeClaim, error) {
	// retrieve all persistent volumes for backup
	ctrl.logger.Infof("Getting PersistentVolumeClaim for backup(%s)", backup.Name)
	pvcs := make([]*corev1.PersistentVolumeClaim, 0)
	for _, resource := range resources {
		pvc := new(corev1.PersistentVolumeClaim)
		err := k8sresource.FromResource(resource, pvc)
		if err != nil {
			ctrl.logger.Warningf("Failed to translate unstructured (%s) to "+
				"pvc. %s", resource.GetName(), err)
			return pvcs, err
		}

		pvcs = append(pvcs, pvc)
	}

	return pvcs, nil
}

//func (ctrl *controller) ensureVolumeBackupContent(
//	backupName string,
//	volBackupParam map[string]string,
//	pvProviderMap map[string][]corev1.PersistentVolume) error {
//	// ensure volume backup content
//
//	for provider, pvList := range pvProviderMap {
//		// check if volume content already available
//		// backup name and provider name is unique tuple for volume backup content
//		vbcList, err := ctrl.volumeBackupClient.List(context.TODO(), metav1.ListOptions{
//			LabelSelector: labels.Set{
//				volumeContentBackupLabel:    backupName,
//				volumeContentVolumeProvider: provider,
//			}.String(),
//		})
//		if err != nil {
//			ctrl.logger.Errorf("Unable to get volume backup content list %s", err)
//			return errors.Wrap(err, "Unable to get volume backup content list")
//		}
//		if len(vbcList.Items) > 0 {
//			continue
//		}
//
//		pvObjectRefs := make([]corev1.PersistentVolume, 0)
//		for _, pv := range pvList {
//			ctrl.logger.Infof("Preparing volume backup content with pv name %s UID %s", pv.Name, pv.UID)
//			pvObjectRefs = append(pvObjectRefs, corev1.PersistentVolume{
//				TypeMeta: metav1.TypeMeta{
//					Kind:       pv.Kind,
//					APIVersion: pv.APIVersion,
//				},
//				ObjectMeta: metav1.ObjectMeta{
//					Namespace: pv.Namespace,
//					Name:      pv.Name,
//					UID:       pv.UID,
//				},
//			})
//		}
//
//		time := metav1.Now()
//		volumeBackupContent := &kahuapi.VolumeBackupContent{
//			ObjectMeta: metav1.ObjectMeta{
//				Labels: map[string]string{
//					volumeContentBackupLabel:    backupName,
//					volumeContentVolumeProvider: provider,
//				},
//				Name: getVBCName(),
//			},
//			Spec: kahuapi.VolumeBackupContentSpec{
//				BackupName:     backupName,
//				Volumes:        pvObjectRefs,
//				VolumeProvider: &provider,
//				Parameters:     volBackupParam,
//			},
//			Status: kahuapi.VolumeBackupContentStatus{
//				Phase:          kahuapi.VolumeBackupContentPhaseInit,
//				StartTimestamp: &time,
//			},
//		}
//
//		_, err = ctrl.volumeBackupClient.Create(context.TODO(), volumeBackupContent, metav1.CreateOptions{})
//		if err != nil {
//			ctrl.logger.Errorf("unable to create volume backup content "+
//				"for provider %s", provider)
//			return errors.Wrap(err, "unable to create volume backup content")
//		}
//	}
//
//	return nil
//}

//func (ctrl *controller) backupVolumeBackupContent(
//	backup *PrepareBackup,
//	backupClient metaservice.MetaService_BackupClient) error {
//	ctrl.logger.Infoln("Starting collecting volume backup content")
//
//	apiResource, _, err := ctrl.discoveryHelper.ByKind(utils.VBC)
//	if err != nil {
//		ctrl.logger.Errorf("Unable to get API resource info for Volume backup content: %s", err)
//		return err
//	}
//	gv := schema.GroupVersion{
//		Group:   apiResource.Group,
//		Version: apiResource.Version,
//	}.String()
//	selectors := labels.Set(map[string]string{
//		volumeContentBackupLabel: backup.Name,
//	}).String()
//
//	list, err := ctrl.volumeBackupClient.List(context.TODO(), metav1.ListOptions{
//		LabelSelector: selectors,
//	})
//	if err != nil {
//		return err
//	}
//	for _, volumeBackupContent := range list.Items {
//		// hack: API resource version and Kind are empty in response
//		// populating with from API discovery module
//		configData, err := json.Marshal(volumeBackupContent.Spec.Parameters)
//		if err != nil {
//			ctrl.logger.Errorf("Unable to get resource content: %s", err)
//			return err
//		}
//
//		metav1.SetMetaDataAnnotation(&volumeBackupContent.ObjectMeta,
//			utils.AnnBackupLocationParam, string(configData))
//		volumeBackupContent.Kind = apiResource.Kind
//		volumeBackupContent.APIVersion = gv
//		resourceData, err := json.Marshal(volumeBackupContent)
//		if err != nil {
//			ctrl.logger.Errorf("Unable to get resource content: %s", err)
//			return err
//		}
//
//		ctrl.logger.Infof("sending metadata for object %s/%s", volumeBackupContent.APIVersion,
//			volumeBackupContent.Name)
//
//		err = backupClient.Send(&metaservice.BackupRequest{
//			Backup: &metaservice.BackupRequest_BackupResource{
//				BackupResource: &metaservice.BackupResource{
//					Resource: &metaservice.Resource{
//						Name:    volumeBackupContent.Name,
//						Group:   volumeBackupContent.APIVersion,
//						Version: volumeBackupContent.APIVersion,
//						Kind:    volumeBackupContent.Kind,
//					},
//					Data: resourceData,
//				},
//			},
//		})
//		if err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

//func (ctrl *controller) backupBackupLocation(
//	backupLocationName string,
//	backupClient metaservice.MetaService_BackupClient) error {
//	ctrl.logger.Infoln("Starting collecting volume backup content")
//
//	apiResource, _, err := ctrl.discoveryHelper.ByKind(utils.BackupLocation)
//	if err != nil {
//		ctrl.logger.Errorf("Unable to get API resource info for Volume backup content: %s", err)
//		return err
//	}
//	gv := schema.GroupVersion{
//		Group:   apiResource.Group,
//		Version: apiResource.Version,
//	}.String()
//
//	location, err := ctrl.backupLocationLister.Get(backupLocationName)
//	if err != nil {
//		return err
//	}
//
//	location.Kind = apiResource.Kind
//	location.APIVersion = gv
//	resourceData, err := json.Marshal(location)
//	if err != nil {
//		ctrl.logger.Errorf("Unable to get resource content: %s", err)
//		return err
//	}
//
//	ctrl.logger.Infof("sending metadata for object %s/%s", location.APIVersion,
//		location.Name)
//
//	//err = backupClient.Send(&metaservice.BackupRequest{
//	//	Backup: &metaservice.BackupRequest_BackupResource{
//	//		BackupResource: &metaservice.BackupResource{
//	//			Resource: &metaservice.Resource{
//	//				Name:    location.Name,
//	//				Group:   apiResource.Group,
//	//				Version: apiResource.Version,
//	//				Kind:    apiResource.Kind,
//	//			},
//	//			Data: resourceData,
//	//		},
//	//	},
//	//})
//
//	return nil
//}

//func (ctrl *controller) annotateBackup(
//	annotation string,
//	backup *kahuapi.Backup) (*kahuapi.Backup, error) {
//	backupName := backup.Name
//	ctrl.logger.Infof("Annotating backup(%s) with %s", backupName, annotation)
//
//	_, ok := backup.Annotations[annotation]
//	if ok {
//		ctrl.logger.Infof("Backup(%s) all-ready annotated with %s", backupName, annotation)
//		return backup, nil
//	}
//
//	backupClone := backup.DeepCopy()
//	metav1.SetMetaDataAnnotation(&backupClone.ObjectMeta, annotation, "true")
//
//	origBytes, err := json.Marshal(backup)
//	if err != nil {
//		return backup, errors.Wrap(err, "error marshalling backup")
//	}
//
//	updatedBytes, err := json.Marshal(backupClone)
//	if err != nil {
//		return backup, errors.Wrap(err, "error marshalling updated backup")
//	}
//
//	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
//	if err != nil {
//		return backup, errors.Wrap(err, "error creating json merge patch for backup")
//	}
//
//	backup, err = ctrl.backupClient.Patch(context.TODO(), backupName,
//		types.MergePatchType, patchBytes, metav1.PatchOptions{})
//	if err != nil {
//		ctrl.logger.Error("Unable to update backup(%s) for volume completeness. %s",
//			backupName, err)
//		errors.Wrap(err, "error annotating volume backup completeness")
//	}
//
//	return backup, nil
//}
