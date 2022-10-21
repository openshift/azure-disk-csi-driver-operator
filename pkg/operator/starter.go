package operator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/openshift/azure-disk-csi-driver-operator/pkg/azurestackhub"

	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"

	opv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/azure-disk-csi-driver-operator/assets"
	configclient "github.com/openshift/client-go/config/clientset/versioned"
	configinformers "github.com/openshift/client-go/config/informers/externalversions"
	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/csi/csicontrollerset"
	"github.com/openshift/library-go/pkg/operator/csi/csidrivercontrollerservicecontroller"
	"github.com/openshift/library-go/pkg/operator/csi/csidrivernodeservicecontroller"
	goc "github.com/openshift/library-go/pkg/operator/genericoperatorclient"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
)

const (
	defaultNamespace         = "openshift-cluster-csi-drivers"
	operatorName             = "azure-disk-csi-driver-operator"
	operandName              = "azure-disk-csi-driver"
	openShiftConfigNamespace = "openshift-config"
	secretName               = "azure-disk-credentials"
	trustedCAConfigMap       = "azure-disk-csi-driver-trusted-ca-bundle"

	ccmOperatorImageEnvName = "CLUSTER_CLOUD_CONTROLLER_MANAGER_OPERATOR_IMAGE"
)

func RunOperator(ctx context.Context, controllerConfig *controllercmd.ControllerContext) error {
	// Create core clientset and informers
	kubeClient := kubeclient.NewForConfigOrDie(rest.AddUserAgent(controllerConfig.KubeConfig, operatorName))
	kubeInformersForNamespaces := v1helpers.NewKubeInformersForNamespaces(kubeClient, defaultNamespace, "", openShiftConfigNamespace)
	nodeInformer := kubeInformersForNamespaces.InformersFor("").Core().V1().Nodes()
	secretInformer := kubeInformersForNamespaces.InformersFor(defaultNamespace).Core().V1().Secrets()
	configMapInformer := kubeInformersForNamespaces.InformersFor(defaultNamespace).Core().V1().ConfigMaps()

	// Create config clientset and informer. This is used to get the cluster ID
	configClient := configclient.NewForConfigOrDie(rest.AddUserAgent(controllerConfig.KubeConfig, operatorName))
	configInformers := configinformers.NewSharedInformerFactory(configClient, 20*time.Minute)

	// Create apiextension client. This is used to verify is a VolumeSnapshotClass CRD exists.
	apiExtClient, err := apiextclient.NewForConfig(rest.AddUserAgent(controllerConfig.KubeConfig, operatorName))
	if err != nil {
		return err
	}

	// Create GenericOperatorclient. This is used by the library-go controllers created down below
	gvr := opv1.SchemeGroupVersion.WithResource("clustercsidrivers")
	operatorClient, dynamicInformers, err := goc.NewClusterScopedOperatorClientWithConfigName(controllerConfig.KubeConfig, gvr, "disk.csi.azure.com")
	if err != nil {
		return err
	}

	dynamicClient, err := dynamic.NewForConfig(controllerConfig.KubeConfig)
	if err != nil {
		return err
	}

	runningOnAzureStackHub, err := azurestackhub.RunningOnAzureStackHub(ctx, configClient.ConfigV1())
	if err != nil {
		return err
	}
	storageClassPath := "storageclass.yaml"
	volumeSnapshotPath := "volumesnapshotclass.yaml"
	if runningOnAzureStackHub {
		klog.Infof("Detected AzureStackHub cloud infrastructure, starting endpoint config sync")
		volumeSnapshotPath = "volumesnapshotclass_ash.yaml"
		storageClassPath = "storageclass_ash.yaml"
		azureStackConfigSyncer, err := azurestackhub.NewAzureStackHubConfigSyncer(
			defaultNamespace,
			openShiftConfigNamespace,
			operatorClient,
			kubeInformersForNamespaces,
			kubeClient,
			controllerConfig.EventRecorder)
		if err != nil {
			return err
		}
		go azureStackConfigSyncer.Run(ctx, 1)
	}

	csiControllerSet := csicontrollerset.NewCSIControllerSet(
		operatorClient,
		controllerConfig.EventRecorder,
	).WithLogLevelController().WithManagementStateController(
		operandName,
		false,
	).WithStaticResourcesController(
		"AzureDiskDriverStaticResourcesController",
		kubeClient,
		dynamicClient,
		kubeInformersForNamespaces,
		assets.ReadFile,
		[]string{
			"controller_sa.yaml",
			"controller_pdb.yaml",
			"node_sa.yaml",
			"csidriver.yaml",
			"service.yaml",
			"cabundle_cm.yaml",
			"rbac/attacher_role.yaml",
			"rbac/attacher_binding.yaml",
			"rbac/privileged_role.yaml",
			"rbac/controller_privileged_binding.yaml",
			"rbac/node_privileged_binding.yaml",
			"rbac/provisioner_role.yaml",
			"rbac/provisioner_binding.yaml",
			"rbac/resizer_role.yaml",
			"rbac/resizer_binding.yaml",
			"rbac/snapshotter_role.yaml",
			"rbac/snapshotter_binding.yaml",
			"rbac/kube_rbac_proxy_role.yaml",
			"rbac/kube_rbac_proxy_binding.yaml",
			"rbac/prometheus_role.yaml",
			"rbac/prometheus_rolebinding.yaml",
		},
	).WithConditionalStaticResourcesController(
		"AzureDiskDriverConditionalStaticResourcesController",
		kubeClient,
		dynamicClient,
		kubeInformersForNamespaces,
		assets.ReadFile,
		[]string{
			volumeSnapshotPath,
		},
		// Only install when CRD exists.
		func() bool {
			name := "volumesnapshotclasses.snapshot.storage.k8s.io"
			_, err := apiExtClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), name, metav1.GetOptions{})
			return err == nil
		},
		// Don't ever remove.
		func() bool {
			return false
		},
	).WithCSIConfigObserverController(
		"AzureDiskDriverCSIConfigObserverController",
		configInformers,
	).WithCSIDriverControllerService(
		"AzureDiskDriverControllerServiceController",
		assetWithImageReplaced(),
		"controller.yaml",
		kubeClient,
		kubeInformersForNamespaces.InformersFor(defaultNamespace),
		configInformers,
		[]factory.Informer{
			nodeInformer.Informer(),
			secretInformer.Informer(),
			configMapInformer.Informer(),
		},
		csidrivercontrollerservicecontroller.WithObservedProxyDeploymentHook(),
		csidrivercontrollerservicecontroller.WithCABundleDeploymentHook(
			defaultNamespace,
			trustedCAConfigMap,
			configMapInformer,
		),
		csidrivercontrollerservicecontroller.WithReplicasHook(nodeInformer.Lister()),
		azurestackhub.WithAzureStackHubDeploymentHook(runningOnAzureStackHub),
		csidrivercontrollerservicecontroller.WithSecretHashAnnotationHook(defaultNamespace, secretName, secretInformer),
	).WithCSIDriverNodeService(
		"AzureDiskDriverNodeServiceController",
		assetWithImageReplaced(),
		"node.yaml",
		kubeClient,
		kubeInformersForNamespaces.InformersFor(defaultNamespace),
		[]factory.Informer{
			secretInformer.Informer(),
		},
		csidrivernodeservicecontroller.WithSecretHashAnnotationHook(defaultNamespace, secretName, secretInformer),
		csidrivernodeservicecontroller.WithObservedProxyDaemonSetHook(),
		csidrivernodeservicecontroller.WithCABundleDaemonSetHook(
			defaultNamespace,
			trustedCAConfigMap,
			configMapInformer,
		),
		azurestackhub.WithAzureStackHubDaemonSetHook(runningOnAzureStackHub),
	).WithServiceMonitorController(
		"AzureDiskServiceMonitorController",
		dynamicClient,
		assets.ReadFile,
		"servicemonitor.yaml",
	).WithStorageClassController(
		"AzureDiskStorageClassController",
		assets.ReadFile,
		storageClassPath,
		kubeClient,
		kubeInformersForNamespaces.InformersFor(""),
	)

	klog.Info("Starting the informers")
	go kubeInformersForNamespaces.Start(ctx.Done())
	go dynamicInformers.Start(ctx.Done())
	go configInformers.Start(ctx.Done())

	klog.Info("Starting controllerset")
	go csiControllerSet.Run(ctx, 1)

	<-ctx.Done()

	return fmt.Errorf("stopped")
}

func assetWithImageReplaced() func(name string) ([]byte, error) {
	return func(name string) ([]byte, error) {
		assetBytes, err := assets.ReadFile(name)
		if err != nil {
			return assetBytes, err
		}
		asset := string(assetBytes)
		asset = strings.ReplaceAll(asset, "${CLUSTER_CLOUD_CONTROLLER_MANAGER_OPERATOR_IMAGE}", os.Getenv(ccmOperatorImageEnvName))
		return []byte(asset), nil
	}
}
