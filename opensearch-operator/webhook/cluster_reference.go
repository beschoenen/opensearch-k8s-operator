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
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

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
