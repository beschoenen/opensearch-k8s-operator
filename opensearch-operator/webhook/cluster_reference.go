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

	opensearchv1 "github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/api/opensearch.org/v1"
	opsterv1 "github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/api/v1"
	"github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/pkg/reconcilers/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// validateClusterReference validates that the referenced OpenSearch cluster exists and that it
// allows resources in resourceNamespace to reference it.
func validateClusterReference(ctx context.Context, c client.Client, ref opensearchv1.OpensearchClusterRef, resourceNamespace string) error {
	key := types.NamespacedName{
		Name:      ref.Name,
		Namespace: util.DetermineClusterNamespace(ref, resourceNamespace),
	}

	// Try new API group first
	var cluster client.Object = &opensearchv1.OpenSearchCluster{}
	if err := c.Get(ctx, key, cluster); err != nil {
		// Fall back to old API group for backward compatibility
		cluster = &opsterv1.OpenSearchCluster{}
		if legacyErr := c.Get(ctx, key, cluster); legacyErr != nil {
			return clusterReferenceError(key.Name, key.Namespace, err, legacyErr)
		}
	}

	if !util.ClusterAllowsNamespace(cluster, resourceNamespace) {
		return errors.New(util.NamespaceNotAllowedMessage(cluster, resourceNamespace))
	}

	return nil
}

// clusterReferenceError builds the error for a cluster reference that could not be resolved.
// errs holds the lookup errors for each API group. The cluster is reported as missing only
// when a lookup returned NotFound; any other error usually means the operator cannot read
// the namespace, for example because it runs in single-namespace watch mode.
func clusterReferenceError(name, namespace string, errs ...error) error {
	for _, err := range errs {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("referenced OpenSearch cluster '%s' in namespace '%s' not found: %w", name, namespace, err)
		}
	}
	return fmt.Errorf("unable to read referenced OpenSearch cluster '%s' in namespace '%s', make sure the operator watches this namespace and has RBAC permissions for it: %w", name, namespace, errors.Join(errs...))
}
