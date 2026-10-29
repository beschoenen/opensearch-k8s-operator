package controllers

import (
	"context"

	opensearchv1 "github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/api/opensearch.org/v1"
	"github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/pkg/reconcilers/util"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// clusterReferencer is a resource that references an OpenSearchCluster.
type clusterReferencer interface {
	client.Object
	GetOpensearchRef() opensearchv1.OpensearchClusterRef
}

// allowedNamespacesChanged only passes cluster updates that change the allowed namespaces annotation.
// Other cluster events are not needed: resources that wait for a cluster already requeue themselves.
var allowedNamespacesChanged = predicate.Funcs{
	CreateFunc:  func(event.CreateEvent) bool { return false },
	DeleteFunc:  func(event.DeleteEvent) bool { return false },
	GenericFunc: func(event.GenericEvent) bool { return false },
	UpdateFunc: func(e event.UpdateEvent) bool {
		return e.ObjectOld.GetAnnotations()[util.AllowedNamespacesAnnotation] !=
			e.ObjectNew.GetAnnotations()[util.AllowedNamespacesAnnotation]
	},
}

// requestsForClusterReferences returns a map function that enqueues every resource in list that
// references the OpenSearchCluster of the event, so that a change to the allowed namespaces of the
// cluster is applied at once instead of on the next requeue.
func requestsForClusterReferences(c client.Client, list client.ObjectList) handler.MapFunc {
	return func(ctx context.Context, cluster client.Object) []reconcile.Request {
		logger := log.FromContext(ctx).WithValues("cluster", client.ObjectKeyFromObject(cluster))

		resources := list.DeepCopyObject().(client.ObjectList)
		if err := c.List(ctx, resources); err != nil {
			logger.Error(err, "failed to list resources that reference the cluster")
			return nil
		}
		items, err := apimeta.ExtractList(resources)
		if err != nil {
			logger.Error(err, "failed to read resources that reference the cluster")
			return nil
		}

		var requests []reconcile.Request
		for _, item := range items {
			resource, ok := item.(clusterReferencer)
			if !ok {
				continue
			}
			ref := resource.GetOpensearchRef()
			if ref.Name == cluster.GetName() &&
				util.DetermineClusterNamespace(ref, resource.GetNamespace()) == cluster.GetNamespace() {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(resource)})
			}
		}
		return requests
	}
}
