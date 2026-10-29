package controllers

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	opensearchv1 "github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/api/opensearch.org/v1"
	"github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/pkg/reconcilers/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Every resource that references a cluster must be found by requestsForClusterReferences.
var (
	_ clusterReferencer = &opensearchv1.OpensearchActionGroup{}
	_ clusterReferencer = &opensearchv1.OpensearchComponentTemplate{}
	_ clusterReferencer = &opensearchv1.OpensearchIndexTemplate{}
	_ clusterReferencer = &opensearchv1.OpenSearchISMPolicy{}
	_ clusterReferencer = &opensearchv1.OpensearchRole{}
	_ clusterReferencer = &opensearchv1.OpensearchSnapshotPolicy{}
	_ clusterReferencer = &opensearchv1.OpensearchTenant{}
	_ clusterReferencer = &opensearchv1.OpensearchUser{}
	_ clusterReferencer = &opensearchv1.OpensearchUserRoleBinding{}
)

var _ = Describe("Cluster reference watch", func() {
	clusterWith := func(annotations map[string]string) *opensearchv1.OpenSearchCluster {
		return &opensearchv1.OpenSearchCluster{ObjectMeta: metav1.ObjectMeta{
			Name: "cluster", Namespace: "cluster-ns", Annotations: annotations,
		}}
	}

	Describe("allowedNamespacesChanged", func() {
		DescribeTable("update events",
			func(old, new map[string]string, expected bool) {
				Expect(allowedNamespacesChanged.Update(event.UpdateEvent{
					ObjectOld: clusterWith(old), ObjectNew: clusterWith(new),
				})).To(Equal(expected))
			},
			Entry("passes an added annotation", nil, map[string]string{util.AllowedNamespacesAnnotation: "team-a"}, true),
			Entry("passes a changed annotation",
				map[string]string{util.AllowedNamespacesAnnotation: "team-a"},
				map[string]string{util.AllowedNamespacesAnnotation: "team-a,team-b"}, true),
			Entry("passes a removed annotation", map[string]string{util.AllowedNamespacesAnnotation: "team-a"}, nil, true),
			Entry("ignores an unchanged annotation",
				map[string]string{util.AllowedNamespacesAnnotation: "team-a"},
				map[string]string{util.AllowedNamespacesAnnotation: "team-a", "other": "changed"}, false),
			Entry("ignores other changes", nil, nil, false),
		)

		It("ignores create, delete and generic events", func() {
			Expect(allowedNamespacesChanged.Create(event.CreateEvent{Object: clusterWith(nil)})).To(BeFalse())
			Expect(allowedNamespacesChanged.Delete(event.DeleteEvent{Object: clusterWith(nil)})).To(BeFalse())
			Expect(allowedNamespacesChanged.Generic(event.GenericEvent{Object: clusterWith(nil)})).To(BeFalse())
		})
	})

	Describe("requestsForClusterReferences", func() {
		It("enqueues every resource that references the cluster", func() {
			tenant := func(namespace, name string, ref opensearchv1.OpensearchClusterRef) client.Object {
				return &opensearchv1.OpensearchTenant{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
					Spec:       opensearchv1.OpensearchTenantSpec{OpensearchRef: ref},
				}
			}
			testScheme := runtime.NewScheme()
			Expect(opensearchv1.AddToScheme(testScheme)).To(Succeed())
			fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(
				tenant("cluster-ns", "same-namespace", opensearchv1.OpensearchClusterRef{Name: "cluster"}),
				tenant("team-a", "other-namespace", opensearchv1.OpensearchClusterRef{Name: "cluster", Namespace: "cluster-ns"}),
				tenant("team-a", "own-namespace-cluster", opensearchv1.OpensearchClusterRef{Name: "cluster"}),
				tenant("team-a", "other-cluster", opensearchv1.OpensearchClusterRef{Name: "other", Namespace: "cluster-ns"}),
				tenant("team-a", "other-cluster-namespace", opensearchv1.OpensearchClusterRef{Name: "cluster", Namespace: "elsewhere"}),
			).Build()

			requests := requestsForClusterReferences(fakeClient, &opensearchv1.OpensearchTenantList{})(context.Background(), clusterWith(nil))

			Expect(requests).To(ConsistOf(
				reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "cluster-ns", Name: "same-namespace"}},
				reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "other-namespace"}},
			))
		})
	})

	// The tenant controller runs in its own manager, because the suite only starts the cluster controller.
	Describe("with the tenant controller", Ordered, func() {
		const (
			clusterNamespace = "watch-cluster"
			tenantNamespace  = "watch-tenant"
		)
		var (
			ctx        context.Context
			stop       context.CancelFunc
			cluster    opensearchv1.OpenSearchCluster
			tenantName = types.NamespacedName{Namespace: tenantNamespace, Name: "tenant"}
		)

		tenantStatus := func(g Gomega) opensearchv1.OpensearchTenantStatus {
			tenant := &opensearchv1.OpensearchTenant{}
			g.Expect(k8sClient.Get(ctx, tenantName, tenant)).To(Succeed())
			return tenant.Status
		}
		// The reconciler records the cluster it manages only after the allowed namespaces check.
		managedCluster := func(g Gomega) *types.UID {
			return tenantStatus(g).ManagedCluster
		}

		BeforeAll(func() {
			ctx, stop = context.WithCancel(context.Background())
			DeferCleanup(func() { stop() })

			manager, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:                 scheme.Scheme,
				Metrics:                metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress: "0",
				Controller:             config.Controller{SkipNameValidation: ptr.To(true)},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect((&OpensearchTenantReconciler{
				Client:   manager.GetClient(),
				Scheme:   manager.GetScheme(),
				Recorder: record.NewFakeRecorder(100),
			}).SetupWithManager(manager, 1)).To(Succeed())
			go func() {
				defer GinkgoRecover()
				Expect(manager.Start(ctx)).To(Succeed())
			}()

			for _, namespace := range []string{clusterNamespace, tenantNamespace} {
				Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
			}
			cluster = ComposeOpensearchCrd("watch-cluster", clusterNamespace)
			Expect(k8sClient.Create(ctx, &cluster)).To(Succeed())
			Expect(k8sClient.Create(ctx, &opensearchv1.OpensearchTenant{
				ObjectMeta: metav1.ObjectMeta{Name: tenantName.Name, Namespace: tenantName.Namespace},
				Spec: opensearchv1.OpensearchTenantSpec{
					OpensearchRef: opensearchv1.OpensearchClusterRef{Name: cluster.Name, Namespace: clusterNamespace},
				},
			})).To(Succeed())
		})

		It("should deny a namespace the cluster does not allow", func() {
			Eventually(func(g Gomega) {
				status := tenantStatus(g)
				g.Expect(status.State).To(Equal(opensearchv1.OpensearchTenantError))
				g.Expect(status.Reason).To(ContainSubstring("is not allowed to reference OpenSearch cluster"))
			}).WithTimeout(10 * time.Second).Should(Succeed())
		})

		It("should reconcile at once when the cluster allows the namespace", func() {
			// The error backoff retries about 21 and 41 seconds after the first denial. Change the
			// annotation between those retries, so that only the cluster watch can explain a
			// reconcile within the time allowed below.
			Consistently(managedCluster).WithTimeout(27 * time.Second).Should(BeNil())

			Eventually(func() error {
				latest := &opensearchv1.OpenSearchCluster{}
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(&cluster), latest); err != nil {
					return err
				}
				latest.Annotations = map[string]string{util.AllowedNamespacesAnnotation: tenantNamespace}
				return k8sClient.Update(ctx, latest)
			}).Should(Succeed())

			Eventually(managedCluster).WithTimeout(3 * time.Second).Should(HaveValue(Equal(cluster.UID)))
		})
	})
})
