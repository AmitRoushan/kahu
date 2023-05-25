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

package manager

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	controllerruntime "sigs.k8s.io/controller-runtime"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	kahuv1beta1 "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/client"
	"github.com/soda-cdm/kahu/client/clientset/versioned"
	"github.com/soda-cdm/kahu/client/informers/externalversions"
	"github.com/soda-cdm/kahu/controllers"
	"github.com/soda-cdm/kahu/controllers/app/config"
	"github.com/soda-cdm/kahu/controllers/backup"
	"github.com/soda-cdm/kahu/controllers/backuplocation"
	"github.com/soda-cdm/kahu/controllers/provider"
	providerReg "github.com/soda-cdm/kahu/controllers/registration/provider"
	"github.com/soda-cdm/kahu/controllers/restore"
	"github.com/soda-cdm/kahu/controllers/snapshot"
	"github.com/soda-cdm/kahu/controllers/snapshot/classsyncer"
	"github.com/soda-cdm/kahu/controllers/snapshot/csi"
	"github.com/soda-cdm/kahu/discovery"
	"github.com/soda-cdm/kahu/framework"
	frameworkmanager "github.com/soda-cdm/kahu/framework/manager"
	"github.com/soda-cdm/kahu/hooks"
	"github.com/soda-cdm/kahu/volume"
)

type ControllerManager struct {
	ctx                      context.Context
	runtimeClient            runtimeclient.Client
	restConfig               *rest.Config
	controllerRuntimeManager manager.Manager
	completeConfig           *config.CompletedConfig
	informerFactory          externalversions.SharedInformerFactory
	kahuClient               versioned.Interface
	kubeClient               kubernetes.Interface
	discoveryHelper          discovery.DiscoveryHelper
	EventBroadcaster         record.EventBroadcaster
	HookExecutor             hooks.Hooks
	PodCommandExecutor       hooks.PodCommandExecutor
	volumeFactory            volume.Interface
	volSnapshotClassSyncer   classsyncer.Interface
	csiSnapshoter            csi.Snapshoter
	framework                framework.Interface
}

func NewControllerManager(ctx context.Context,
	completeConfig *config.CompletedConfig,
	clientFactory client.Factory,
	informerFactory externalversions.SharedInformerFactory) (*ControllerManager, error) {

	scheme := runtime.NewScheme()
	kahuv1beta1.AddToScheme(scheme)

	clientConfig, err := clientFactory.ClientConfig()
	if err != nil {
		return nil, err
	}

	volumeFactory, err := volume.NewVolumeHandler(ctx, clientFactory)
	if err != nil {
		return nil, err
	}

	volSnapshotClassSyncer, err := classsyncer.NewSnapshotClassSync(ctx,
		clientConfig,
		completeConfig.KubeClient)
	if err != nil {
		return nil, err
	}

	csiSnapshoter, err := csi.NewSnapshotter(ctx,
		clientConfig,
		completeConfig.KubeClient,
		completeConfig.KahuClient,
		volSnapshotClassSyncer)
	if err != nil {
		return nil, err
	}

	ctrlRuntimeManager, err := controllerruntime.NewManager(clientConfig, controllerruntime.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, err
	}

	frmwork, err := frameworkmanager.NewFramework(ctx,
		completeConfig.FrameworkConfig,
		completeConfig.KubeClient,
		completeConfig.KahuClient.KahuV1beta1(),
		completeConfig.DynamicClient,
		completeConfig.DiscoveryHelper,
		completeConfig.EventBroadcaster)
	if err != nil {
		return nil, err
	}

	return &ControllerManager{
		ctx:                      ctx,
		runtimeClient:            ctrlRuntimeManager.GetClient(),
		restConfig:               clientConfig,
		controllerRuntimeManager: ctrlRuntimeManager,
		completeConfig:           completeConfig,
		kahuClient:               completeConfig.KahuClient,
		kubeClient:               completeConfig.KubeClient,
		informerFactory:          informerFactory,
		discoveryHelper:          completeConfig.DiscoveryHelper,
		EventBroadcaster:         completeConfig.EventBroadcaster,
		HookExecutor:             completeConfig.HookExecutor,
		PodCommandExecutor:       completeConfig.PodCmdExecutor,
		volumeFactory:            volumeFactory,
		volSnapshotClassSyncer:   volSnapshotClassSyncer,
		csiSnapshoter:            csiSnapshoter,
		framework:                frmwork,
	}, nil
}

func (mgr *ControllerManager) InitControllers() (map[string]controllers.Controller, error) {
	availableControllers := make(map[string]controllers.Controller, 0)

	// add controllers here
	// integrate backup controller

	backupController, err := backup.NewController(
		mgr.ctx,
		mgr.completeConfig.BackupControllerConfig,
		mgr.kubeClient,
		mgr.kahuClient,
		mgr.completeConfig.DynamicClient,
		mgr.informerFactory,
		mgr.EventBroadcaster,
		mgr.completeConfig.DiscoveryHelper,
		mgr.HookExecutor,
		mgr.volumeFactory,
		mgr.framework)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize backup controller. %s", err)
	}
	availableControllers[backupController.Name()] = backupController

	// integrate restore controller
	restoreController, err := restore.NewController(
		mgr.ctx,
		mgr.completeConfig.RestoreControllerConfig,
		mgr.completeConfig.KubeClient,
		mgr.kahuClient,
		mgr.completeConfig.DynamicClient,
		mgr.completeConfig.DiscoveryHelper,
		mgr.informerFactory,
		mgr.PodCommandExecutor,
		mgr.kubeClient.CoreV1().RESTClient(),
		mgr.framework,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize restore controller. %s", err)
	}
	availableControllers[restoreController.Name()] = restoreController

	// integrate snapshot controller
	volumeSnapshotController, err := snapshot.NewController(
		mgr.ctx,
		mgr.completeConfig.KubeClient,
		mgr.kahuClient,
		mgr.completeConfig.DynamicClient,
		mgr.informerFactory,
		mgr.EventBroadcaster,
		mgr.completeConfig.DiscoveryHelper,
		mgr.volSnapshotClassSyncer,
		mgr.csiSnapshoter,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize snapshot controller. %s", err)
	}
	availableControllers[volumeSnapshotController.Name()] = volumeSnapshotController

	// integrate provider registration controller
	providerRegController, err := providerReg.NewController(
		mgr.ctx,
		mgr.completeConfig.KubeClient,
		mgr.kahuClient,
		mgr.informerFactory,
		mgr.EventBroadcaster,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize provider registration controller. %s", err)
	}
	availableControllers[providerRegController.Name()] = providerRegController

	// integrate provider controller
	providerController, err := provider.NewController(
		mgr.ctx,
		mgr.kahuClient,
		mgr.informerFactory,
		mgr.EventBroadcaster,
		mgr.volumeFactory,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize backup location controller. %s", err)
	}
	availableControllers[providerController.Name()] = providerController

	// integrate backup location controller
	backupLocationController, err := backuplocation.NewController(
		mgr.ctx,
		mgr.completeConfig.KubeClient,
		mgr.kahuClient,
		mgr.informerFactory,
		mgr.EventBroadcaster,
		mgr.framework,
		mgr.volumeFactory,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize backup location controller. %s", err)
	}
	availableControllers[backupLocationController.Name()] = backupLocationController

	log.Infof("Available controllers %+v", availableControllers)

	return availableControllers, nil
}

func (mgr *ControllerManager) RemoveDisabledControllers(controllers map[string]controllers.Controller) error {
	for _, controllerName := range mgr.completeConfig.DisableControllers {
		if _, ok := controllers[controllerName]; ok {
			log.Infof("Disabling controller: %s", controllerName)
			delete(controllers, controllerName)
		}
	}
	return nil
}

func (mgr *ControllerManager) RunControllers(controllerMap map[string]controllers.Controller) error {
	for _, ctrl := range controllerMap {
		mgr.controllerRuntimeManager.
			Add(manager.RunnableFunc(func(c controllers.Controller, workers int) func(ctx context.Context) error {
				return func(ctx context.Context) error {
					return c.Run(ctx, mgr.completeConfig.ControllerWorkers)
				}
			}(ctrl, mgr.completeConfig.ControllerWorkers)))
	}

	log.Info("Controllers starting...")
	return mgr.controllerRuntimeManager.Start(mgr.ctx)
}
