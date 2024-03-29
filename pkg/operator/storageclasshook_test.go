package operator

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	opv1 "github.com/openshift/api/operator/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	provisionerName       = "disk.csi.azure.com"
	testSubscriptionID    = "f2007bbf-f802-4a47-9336-cf7c6b89b378"
	testResourceGroup     = "test-rg"
	testEncryptionSetName = "test-encset"
)

func getExpectedSCParam(sub string, rg string, name string) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/diskEncryptionSets/%s", sub, rg, name)
}

func sc() *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: nil,
		},
		Parameters: map[string]string{
			"skuname": "Premium_LRS",
		},
		Provisioner: provisionerName,
	}
}

func withParameters(sc *storagev1.StorageClass, keysAndValues ...string) *storagev1.StorageClass {
	for i := 0; i < len(keysAndValues); i += 2 {
		sc.Parameters[keysAndValues[i]] = keysAndValues[i+1]
	}
	return sc
}

func TestStorageClassHook(t *testing.T) {
	tests := []struct {
		name        string
		driver      *opv1.ClusterCSIDriver
		inputSC     *storagev1.StorageClass
		expectedSC  *storagev1.StorageClass
		expectError bool
	}{
		{
			name: "invalid provisioner",
			driver: &opv1.ClusterCSIDriver{
				Spec: opv1.ClusterCSIDriverSpec{},
			},
			inputSC: &storagev1.StorageClass{
				Provisioner: "invalid-provisioner",
			},
			expectedSC: &storagev1.StorageClass{
				Provisioner: "invalid-provisioner",
			},
			expectError: true,
		},
		{
			name: "no driver config",
			driver: &opv1.ClusterCSIDriver{
				Spec: opv1.ClusterCSIDriverSpec{},
			},
			inputSC:    sc(),
			expectedSC: sc(),
		},
		{
			name: "driver config with irrelevant type",
			driver: &opv1.ClusterCSIDriver{
				Spec: opv1.ClusterCSIDriverSpec{
					DriverConfig: opv1.CSIDriverConfigSpec{
						DriverType: opv1.AWSDriverType,
					},
				},
			},
			inputSC:    sc(),
			expectedSC: sc(),
		},
		{
			name: "driver config with no spec",
			driver: &opv1.ClusterCSIDriver{
				Spec: opv1.ClusterCSIDriverSpec{
					DriverConfig: opv1.CSIDriverConfigSpec{
						DriverType: opv1.AzureDriverType,
					},
				},
			},
			inputSC:    sc(),
			expectedSC: sc(),
		},
		{
			name: "driver config with nil DiskEncryptionSet",
			driver: &opv1.ClusterCSIDriver{
				Spec: opv1.ClusterCSIDriverSpec{
					DriverConfig: opv1.CSIDriverConfigSpec{
						DriverType: opv1.AzureDriverType,
						Azure: &opv1.AzureCSIDriverConfigSpec{
							DiskEncryptionSet: nil,
						},
					},
				},
			},
			inputSC:    sc(),
			expectedSC: sc(),
		},
		{
			name: "with diskEncryptionSetID in SC",
			driver: &opv1.ClusterCSIDriver{
				Spec: opv1.ClusterCSIDriverSpec{
					DriverConfig: opv1.CSIDriverConfigSpec{
						DriverType: opv1.AzureDriverType,
						Azure: &opv1.AzureCSIDriverConfigSpec{
							DiskEncryptionSet: &opv1.AzureDiskEncryptionSet{
								SubscriptionID: testSubscriptionID,
								ResourceGroup:  testResourceGroup,
								Name:           testEncryptionSetName,
							},
						},
					},
				},
			},
			inputSC:    sc(),
			expectedSC: withParameters(sc(), diskEncryptionSetID, getExpectedSCParam(testSubscriptionID, testResourceGroup, testEncryptionSetName)),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ccdLister := &fakeCCDLister{test.driver}
			hook := getKMSKeyHook(ccdLister)
			err := hook(nil, test.inputSC)

			if err != nil && !test.expectError {
				t.Errorf("got unexpected error: %s", err)
			}
			if err == nil && test.expectError {
				t.Errorf("expected error, got none")
			}
			if !equality.Semantic.DeepEqual(test.expectedSC, test.inputSC) {
				t.Errorf("Unexpected StorageClass content:\n%s", cmp.Diff(test.expectedSC, test.inputSC))
			}
		})
	}
}

type fakeCCDLister struct {
	driver *opv1.ClusterCSIDriver
}

func (f fakeCCDLister) List(selector labels.Selector) (ret []*opv1.ClusterCSIDriver, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (f fakeCCDLister) Get(name string) (*opv1.ClusterCSIDriver, error) {
	if name != provisionerName {
		return nil, fmt.Errorf("ClusterCSIDriver %q not found", name)
	}
	return f.driver, nil
}
