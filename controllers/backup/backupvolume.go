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

	"github.com/google/uuid"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
)

func getVBCName(backupName string) string {
	return fmt.Sprintf("%s-%s", backupName, uuid.New().String())
}

func (ctrl *controller) processVolumeBackup(backup *kahuapi.Backup, ctx Context) error {
	// check backup state
	// process backup for metadata and volume backup
	ctrl.logger.Infof("Processing Volume backup(%s)", backup.Name)

	uPVCResources := ctx.GetKindResources(PVCKind)
	pvProviderMap := make(map[string][]v1.PersistentVolume, 0)
	for _, uPVCResource := range uPVCResources {
		var pvc v1.PersistentVolumeClaim
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(uPVCResource.Object, &pvc)
		if err != nil {
			ctrl.logger.Warningf("Failed to translate unstructured (%s) to "+
				"pvc. %s", uPVCResource.GetName(), err)
			continue
		}

		if len(pvc.Spec.VolumeName) == 0 ||
			pvc.DeletionTimestamp != nil {
			// ignore unbound PV
			continue
		}
		pv, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Get(context.TODO(),
			pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			ctrl.logger.Warningf("unable to get PV %s", pvc.Spec.VolumeName)
			return err
		}

		if pv.Spec.CSI == nil {
			// ignoring non CSI Volumes
			continue
		}

		pvList, ok := pvProviderMap[pv.Spec.CSI.Driver]
		if !ok {
			pvList = make([]v1.PersistentVolume, 0)
		}
		pvList = append(pvList, *pv)
		pvProviderMap[pv.Spec.CSI.Driver] = pvList
	}

	for provider, pvList := range pvProviderMap {
		time := metav1.Now()
		_, err := ctrl.volumeContentClient.Create(context.TODO(), &kahuapi.VolumeBackupContent{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					volumeContentBackupLabel: backup.Name,
				},
				Name: getVBCName(backup.Name),
			},
			Spec: kahuapi.VolumeBackupContentSpec{
				BackupName:     backup.Name,
				Volumes:        pvList,
				VolumeProvider: &provider,
			},
			Status: kahuapi.VolumeBackupContentStatus{
				Phase:          kahuapi.VolumeBackupContentPhaseInit,
				StartTimestamp: &time,
			},
		}, metav1.CreateOptions{})
		if err != nil {
			ctrl.logger.Errorf("unable to create volume backup content "+
				"for provider %s", provider)
			return errors.Wrap(err, "unable to create volume backup content")
		}
	}

	return nil
}
