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
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/pkg/errors"
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	pb "github.com/soda-cdm/kahu/providerframework/metaservice/lib/go"
)

func (ctrl *controller) processMetadataBackup(backup *kahuapi.Backup, ctx Context) error {
	// process backup for metadata and volume backup
	ctrl.logger.Infof("Processing Metadata backup(%s)", backup.Name)

	metaservice, grpcConn, err := ctrl.fetchMetaServiceClient(backup.Spec.MetadataLocation)
	if err != nil {
		return err
	}
	defer grpcConn.Close()

	backupClient, err := metaservice.Backup(context.Background())
	if err != nil {
		return errors.Wrap(err, "unable to get backup client")
	}
	defer backupClient.CloseAndRecv()

	err = backupClient.Send(&pb.BackupRequest{
		Backup: &pb.BackupRequest_Identifier{
			Identifier: &pb.BackupIdentifier{
				BackupHandle: backup.Name,
			},
		},
	})
	if err != nil {
		ctrl.logger.Errorf("Unable to connect metadata service %s", err)
		return err
	}

	resources := ctx.GetResources()
	for _, resource := range resources {
		data, err := resource.MarshalJSON()
		if err != nil {
			return err
		}
		gv, err := schema.ParseGroupVersion(resource.GetAPIVersion())
		if err != nil {
			return errors.Wrapf(err, "unable to parse GroupVersion %s", resource.GetAPIVersion())
		}

		err = backupClient.Send(&pb.BackupRequest{
			Backup: &pb.BackupRequest_BackupResource{
				BackupResource: &pb.BackupResource{
					Resource: &pb.Resource{
						Name:    resource.GetName(),
						Group:   gv.Group,
						Version: gv.Version,
						Kind:    resource.GetKind(),
					},
					Data: data,
				},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
