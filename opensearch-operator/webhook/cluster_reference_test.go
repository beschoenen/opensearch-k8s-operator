/*
Copyright 2021.

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

package webhook

import (
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	opensearchv1 "github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/api/opensearch.org/v1"
	opsterv1 "github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	resourceNamespace  = "resource-ns"
	clusterNamespace   = "cluster-ns"
	unwatchedNamespace = "unwatched-ns"
)

// clusterRefCase describes a validator for a resource that references an OpenSearch cluster.
type clusterRefCase struct {
	kind      string
	validator func(c client.Client) admission.CustomValidator
	// object returns a resource that passes all other validation for the given reference.
	object func(ref opensearchv1.OpensearchClusterRef) runtime.Object
}

func clusterRefMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: "test-resource", Namespace: resourceNamespace}
}

var clusterRefCases = []clusterRefCase{
	{
		kind: "OpensearchActionGroup",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchActionGroupValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpensearchActionGroup{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpensearchActionGroupSpec{
				OpensearchRef:  ref,
				AllowedActions: []string{"indices:data/read/*"},
			}}
		},
	},
	{
		kind: "OpensearchComponentTemplate",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchComponentTemplateValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpensearchComponentTemplate{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpensearchComponentTemplateSpec{
				OpensearchRef: ref,
			}}
		},
	},
	{
		kind: "OpensearchIndexTemplate",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchIndexTemplateValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpensearchIndexTemplate{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpensearchIndexTemplateSpec{
				OpensearchRef: ref,
			}}
		},
	},
	{
		kind: "OpenSearchISMPolicy",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchISMPolicyValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpenSearchISMPolicy{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpenSearchISMPolicySpec{
				OpensearchRef: ref,
				DefaultState:  "hot",
				States:        []opensearchv1.State{{Name: "hot"}},
			}}
		},
	},
	{
		kind: "OpensearchRole",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchRoleValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpensearchRole{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpensearchRoleSpec{
				OpensearchRef:      ref,
				ClusterPermissions: []string{"cluster_monitor"},
			}}
		},
	},
	{
		kind: "OpensearchSnapshotPolicy",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchSnapshotPolicyValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpensearchSnapshotPolicy{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpensearchSnapshotPolicySpec{
				OpensearchRef:  ref,
				PolicyName:     "daily",
				SnapshotConfig: opensearchv1.SnapshotConfig{Repository: "backups"},
				Creation: opensearchv1.SnapshotCreation{
					Schedule: opensearchv1.CronSchedule{Cron: opensearchv1.CronExpression{Expression: "0 0 * * *"}},
				},
			}}
		},
	},
	{
		kind: "OpensearchTenant",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchTenantValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpensearchTenant{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpensearchTenantSpec{
				OpensearchRef: ref,
			}}
		},
	},
	{
		kind: "OpensearchUser",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchUserValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpensearchUser{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpensearchUserSpec{
				OpensearchRef: ref,
				PasswordFrom: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "user-password"},
					Key:                  "password",
				},
			}}
		},
	},
	{
		kind: "OpensearchUserRoleBinding",
		validator: func(c client.Client) admission.CustomValidator {
			return &OpenSearchUserRoleBindingValidator{Client: c}
		},
		object: func(ref opensearchv1.OpensearchClusterRef) runtime.Object {
			return &opensearchv1.OpensearchUserRoleBinding{ObjectMeta: clusterRefMeta(), Spec: opensearchv1.OpensearchUserRoleBindingSpec{
				OpensearchRef: ref,
				Roles:         []string{"readall"},
				Users:         []string{"test-user"},
			}}
		},
	},
}

var _ = Describe("Cluster references", func() {
	for _, tc := range clusterRefCases {
		Describe(tc.kind, func() {
			var (
				ctx       context.Context
				validator admission.CustomValidator
			)

			ref := func(name, namespace string) opensearchv1.OpensearchClusterRef {
				return opensearchv1.OpensearchClusterRef{Name: name, Namespace: namespace}
			}

			BeforeEach(func() {
				ctx = context.Background()
				scheme := runtime.NewScheme()
				Expect(opensearchv1.AddToScheme(scheme)).To(Succeed())
				Expect(opsterv1.AddToScheme(scheme)).To(Succeed())

				clusters := []client.Object{
					&opensearchv1.OpenSearchCluster{ObjectMeta: metav1.ObjectMeta{Name: "local-cluster", Namespace: resourceNamespace}},
					&opensearchv1.OpenSearchCluster{ObjectMeta: metav1.ObjectMeta{Name: "remote-cluster", Namespace: clusterNamespace}},
				}

				// Simulate the cache error that a namespace-scoped operator returns for other namespaces.
				fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(clusters...).
					WithInterceptorFuncs(interceptor.Funcs{
						Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
							if key.Namespace == unwatchedNamespace {
								return fmt.Errorf("unable to get: %v because of unknown namespace for the cache", key)
							}
							return c.Get(ctx, key, obj, opts...)
						},
					}).Build()

				validator = tc.validator(fakeClient)
			})

			Describe("ValidateCreate", func() {
				It("should allow a cluster in the resource namespace", func() {
					_, err := validator.ValidateCreate(ctx, tc.object(ref("local-cluster", "")))
					Expect(err).NotTo(HaveOccurred())
				})

				It("should allow a cluster in another namespace", func() {
					_, err := validator.ValidateCreate(ctx, tc.object(ref("remote-cluster", clusterNamespace)))
					Expect(err).NotTo(HaveOccurred())
				})

				It("should not look for the cluster in the resource namespace when a namespace is set", func() {
					_, err := validator.ValidateCreate(ctx, tc.object(ref("local-cluster", clusterNamespace)))
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("'local-cluster' in namespace '%s' not found", clusterNamespace)))
				})

				It("should reject a missing cluster in another namespace", func() {
					_, err := validator.ValidateCreate(ctx, tc.object(ref("missing-cluster", clusterNamespace)))
					Expect(err).To(HaveOccurred())
					Expect(apierrors.IsNotFound(errors.Unwrap(err))).To(BeTrue())
					Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("'missing-cluster' in namespace '%s' not found", clusterNamespace)))
				})

				It("should report a namespace the operator cannot read", func() {
					_, err := validator.ValidateCreate(ctx, tc.object(ref("remote-cluster", unwatchedNamespace)))
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("make sure the operator watches this namespace"))
					Expect(err.Error()).To(ContainSubstring("unknown namespace for the cache"))
					Expect(err.Error()).NotTo(ContainSubstring("not found"))
				})
			})

			Describe("ValidateUpdate", func() {
				DescribeTable("cluster reference changes",
					func(oldRef, newRef opensearchv1.OpensearchClusterRef, allowed bool) {
						_, err := validator.ValidateUpdate(ctx, tc.object(oldRef), tc.object(newRef))
						if allowed {
							Expect(err).NotTo(HaveOccurred())
						} else {
							Expect(err).To(HaveOccurred())
							Expect(err.Error()).To(ContainSubstring("cannot change the cluster"))
						}
					},
					Entry("should allow an unchanged reference",
						ref("remote-cluster", clusterNamespace), ref("remote-cluster", clusterNamespace), true),
					Entry("should allow setting the namespace to the resource namespace",
						ref("local-cluster", ""), ref("local-cluster", resourceNamespace), true),
					Entry("should allow clearing the namespace when it is the resource namespace",
						ref("local-cluster", resourceNamespace), ref("local-cluster", ""), true),
					Entry("should reject a name change",
						ref("local-cluster", ""), ref("other-cluster", ""), false),
					Entry("should reject a namespace change",
						ref("remote-cluster", "team-a"), ref("remote-cluster", "team-b"), false),
					Entry("should reject moving to another namespace",
						ref("remote-cluster", ""), ref("remote-cluster", clusterNamespace), false),
				)
			})
		})
	}
})

var _ = Describe("clusterReferenceError", func() {
	notFound := apierrors.NewNotFound(schema.GroupResource{Group: "opensearch.org", Resource: "opensearchclusters"}, "cluster")
	noMatch := errors.New("no matches for kind \"OpenSearchCluster\" in version \"opensearch.opster.io/v1\"")
	cacheErr := errors.New("unable to get: cluster-ns/cluster because of unknown namespace for the cache")

	It("should report a missing cluster when the legacy API group is not installed", func() {
		err := clusterReferenceError("cluster", "cluster-ns", notFound, noMatch)
		Expect(err.Error()).To(ContainSubstring("not found"))
		Expect(errors.Is(err, notFound)).To(BeTrue())
	})

	It("should report an unreadable namespace and keep both errors", func() {
		err := clusterReferenceError("cluster", "cluster-ns", cacheErr, cacheErr)
		Expect(err.Error()).To(ContainSubstring("make sure the operator watches this namespace"))
		Expect(errors.Is(err, cacheErr)).To(BeTrue())
	})
})
