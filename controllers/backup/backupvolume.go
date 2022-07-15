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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
)

func getVBCName(backupName string) string {
	return fmt.Sprintf("%s-%s", backupName, uuid.New().String())
}

func (ctrl *controller) processVolumeBackup(backup *kahuapi.Backup, ctx Context) error {
	pvProviderMap := make(map[string][]corev1.PersistentVolume, 0)
	ctrl.logger.Infof("Processing Volume backup(%s)", backup.Name)

	pvs, err := ctrl.getVolumes(backup, ctx)
	if err != nil {
		return err
	}

	// group pv with providers
	for _, pv := range pvs {
		pvList, ok := pvProviderMap[pv.Spec.CSI.Driver]
		if !ok {
			pvList = make([]corev1.PersistentVolume, 0)
		}
		pvList = append(pvList, pv)
		pvProviderMap[pv.Spec.CSI.Driver] = pvList
	}

	// ensure volume backup content
	return ctrl.ensureVolumeBackupContent(backup.Name, pvProviderMap)
}

func (ctrl *controller) removeVolumeBackup(
	backup *kahuapi.Backup) error {

	vbcList, err := ctrl.volumeContentClient.List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.Set{
			volumeContentBackupLabel: backup.Name,
		}.String(),
	})
	if err != nil {
		ctrl.logger.Errorf("Unable to get volume backup content list %s", err)
		return errors.Wrap(err, "Unable to get volume backup content list")
	}

	for _, vbc := range vbcList.Items {
		err := ctrl.volumeContentClient.Delete(context.TODO(), vbc.Name, metav1.DeleteOptions{})
		if err != nil {
			ctrl.logger.Errorf("Failed to delete volume backup content %s", err)
			return errors.Wrap(err, "Unable to delete volume backup content")
		}
	}

	return nil
}

func (ctrl *controller) getVolumes(
	backup *kahuapi.Backup,
	ctx Context) ([]corev1.PersistentVolume, error) {
	// retrieve all persistent volumes for backup
	ctrl.logger.Infof("Getting PersistentVolume for backup(%s)", backup.Name)

	// check if volume backup content already available
	unstructuredPVCs := ctx.GetKindResources(PVCKind)
	unstructuredPVs := ctx.GetKindResources(PVKind)
	pvs := make([]corev1.PersistentVolume, 0)
	// list all PVs
	pvNames := sets.NewString()
	for _, unstructuredPV := range unstructuredPVs {
		var pv corev1.PersistentVolume
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPV.Object, &pv)
		if err != nil {
			ctrl.logger.Errorf("Failed to translate unstructured (%s) to "+
				"pv. %s", unstructuredPV.GetName(), err)
			return pvs, err
		}
		pvs = append(pvs, pv)
		pvNames.Insert(pv.Name)
	}

	for _, unstructuredPVC := range unstructuredPVCs {
		var pvc corev1.PersistentVolumeClaim
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPVC.Object, &pvc)
		if err != nil {
			ctrl.logger.Warningf("Failed to translate unstructured (%s) to "+
				"pvc. %s", unstructuredPVC.GetName(), err)
			return pvs, err
		}

		if len(pvc.Spec.VolumeName) == 0 ||
			pvc.DeletionTimestamp != nil {
			// ignore unbound PV
			continue
		}
		k8sPV, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Get(context.TODO(),
			pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			ctrl.logger.Errorf("unable to get PV %s", pvc.Spec.VolumeName)
			return pvs, err
		}

		if k8sPV.Spec.CSI == nil || // ignore if not CSI
			pvNames.Has(k8sPV.Name) { // ignore if all-ready considered
			// ignoring non CSI Volumes
			continue
		}

		var pv corev1.PersistentVolume
		k8sPV.DeepCopyInto(&pv)
		pvs = append(pvs, pv)
		pvNames.Insert(pv.Name)
	}

	return pvs, nil
}

func (ctrl *controller) ensureVolumeBackupContent(
	backupName string,
	pvProviderMap map[string][]corev1.PersistentVolume) error {
	// ensure volume backup content
	for provider, pvList := range pvProviderMap {
		// check if volume content already available
		// backup name and provider name is unique tuple for volume backup content
		vbcList, err := ctrl.volumeContentClient.List(context.TODO(), metav1.ListOptions{
			LabelSelector: labels.Set{
				volumeContentBackupLabel:    backupName,
				volumeContentVolumeProvider: provider,
			}.String(),
		})
		if err != nil {
			ctrl.logger.Errorf("Unable to get volume backup content list %s", err)
			return errors.Wrap(err, "Unable to get volume backup content list")
		}
		if len(vbcList.Items) > 0 {
			continue
		}
		time := metav1.Now()
		volumeBackupContent := &kahuapi.VolumeBackupContent{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					volumeContentBackupLabel:    backupName,
					volumeContentVolumeProvider: provider,
				},
				Name: getVBCName(backupName),
			},
			Spec: kahuapi.VolumeBackupContentSpec{
				BackupName:     backupName,
				Volumes:        pvList,
				VolumeProvider: &provider,
			},
			Status: kahuapi.VolumeBackupContentStatus{
				Phase:          kahuapi.VolumeBackupContentPhaseInit,
				StartTimestamp: &time,
			},
		}

		_, err = ctrl.volumeContentClient.Create(context.TODO(), volumeBackupContent, metav1.CreateOptions{})
		if err != nil {
			ctrl.logger.Errorf("unable to create volume backup content "+
				"for provider %s", provider)
			return errors.Wrap(err, "unable to create volume backup content")
		}
	}

	return nil
}
