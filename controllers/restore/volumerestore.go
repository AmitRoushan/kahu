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

package restore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/utils"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"reflect"
)

func (ctx *restoreContext) syncVolumeRestore(restore *kahuapi.Restore, indexer cache.Indexer) (err error) {
	// all contents are prefetched based on restore backup spec
	// check if volume backup required
	pvcs, err := indexer.ByIndex(backupObjectResourceIndex, utils.PVC)
	if err != nil || len(pvcs) == 0 { // error if pvc not cached
		ctx.logger.Info("PVCs are not available for restore. Continue with metadata restore")
		// add volume backup content in resource backup list
		return ctx.syncMetadataRestore(restore, indexer)
	}
	if metav1.HasAnnotation(restore.ObjectMeta, annVolumeRestoreCompleted) {
		restore.Status.Stage = kahuapi.RestoreStageVolumes
		restore, err = ctx.updateRestoreStatus(restore)
		if err != nil {
			ctx.logger.Errorf("Unable to update restore status. %s", err)
			return err
		}

		// add volume backup content in resource backup list
		return ctx.syncMetadataRestore(restore, indexer)
	}

	// check if restore has already scheduled for volume restore
	// if already scheduled for restore, continue to wait for completion
	restoreVolContents, err := ctx.restoreVolumeClient.List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		ctx.logger.Errorf("Unable to get volume restore content. %s", err)
		return err
	}
	if len(pvcs) == len(restoreVolContents.Items) {
		ctx.logger.Info("Volume restore already scheduled. Continue to wait for restore")
		return nil
	}

	return ctx.volumeRestore(restore, indexer, restoreVolContents.Items, pvcs)
}

func (ctx *restoreContext) volumeRestore(
	restore *kahuapi.Restore,
	indexer cache.Indexer,
	restoreVolContents []kahuapi.VolumeRestoreContent,
	pvcObjects []interface{}) error {
	// all contents are prefetched based on restore backup spec
	ctx.logger.Infof("Restore in %s phase", kahuapi.RestoreStageVolumes)

	pvcs := make([]*v1.PersistentVolumeClaim, 0)
	for _, pvcObject := range pvcObjects {
		var unstructuredPVC *unstructured.Unstructured
		switch unstructuredResource := pvcObject.(type) {
		case *unstructured.Unstructured:
			unstructuredPVC = unstructuredResource
		case unstructured.Unstructured:
			unstructuredPVC = unstructuredResource.DeepCopy()
		default:
			ctx.logger.Warningf("Unknown cached resource type. %s", reflect.TypeOf(pvcObject))
			continue
		}
		pvc := new(v1.PersistentVolumeClaim)
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPVC.Object, &pvc)
		if err != nil {
			ctx.logger.Errorf("Failed to translate unstructured (%s) to "+
				"pv. %s", unstructuredPVC.GetName(), err)
			return errors.Wrap(err, "Failed to covert unstructured resource to PVC for resolution")
		}

		pvcs = append(pvcs, pvc)
	}

	err := ctx.ensureStorageClass(indexer, pvcs)
	if err != nil {
		return err
	}

	return nil
}

func (ctx *restoreContext) ensureStorageClass(
	indexer cache.Indexer,
	pvcs []*v1.PersistentVolumeClaim) error {
	// all contents are prefetched based on restore backup spec
	ctx.logger.Infof("restoring storage class in %s phase", kahuapi.RestoreStageVolumes)

	scObjects, err := indexer.ByIndex(backupObjectResourceIndex, utils.SC)
	if err != nil {
		return errors.Wrap(err, "storage class not available in restore cache")
	}
	storageClasses := make([]*unstructured.Unstructured, 0)
	for _, scObject := range scObjects {
		var unstructuredSC *unstructured.Unstructured
		switch unstructuredResource := scObject.(type) {
		case *unstructured.Unstructured:
			unstructuredSC = unstructuredResource
		case unstructured.Unstructured:
			unstructuredSC = unstructuredResource.DeepCopy()
		default:
			ctx.logger.Warningf("Unknown cached resource type. %s", reflect.TypeOf(scObject))
			continue
		}
		storageClasses = append(storageClasses, unstructuredSC)
		sc := new(storagev1.StorageClass)
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredSC.Object, &sc)
		if err != nil {
			ctx.logger.Errorf("Failed to translate unstructured (%s) to "+
				"sc. %s", unstructuredSC.GetName(), err)
			return errors.Wrap(err, "Failed to covert unstructured resource to PVC for resolution")
		}

		storageClasses = append(storageClasses, unstructuredSC)
	}

	for _, pvc := range pvcs {
		found := false
		for _, storageclass := range storageClasses {
			if storageclass.GetName() == *pvc.Spec.StorageClassName {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("restore cache donot have storage class (%s).Check backup data",
				*pvc.Spec.StorageClassName)
		}
	}

	for _, storageclass := range storageClasses {
		err := ctx.applyResource(storageclass)
		if err != nil {
			ctx.logger.Errorf("Unable to create storage class resource, %s", err)
			return errors.Wrap(err, "Unable to create storage class resource")
		}
	}

	return nil
}
