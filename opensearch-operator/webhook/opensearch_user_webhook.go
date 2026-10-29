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
	"fmt"

	opensearchv1 "github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/api/opensearch.org/v1"
	"github.com/opensearch-project/opensearch-k8s-operator/opensearch-operator/pkg/reconcilers/util"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

//+kubebuilder:webhook:path=/validate-opensearch-org-v1-opensearchuser,mutating=false,failurePolicy=fail,sideEffects=None,groups=opensearch.org,resources=opensearchusers,verbs=create;update,versions=v1,name=vopensearchuser.opensearch.org,admissionReviewVersions=v1

type OpenSearchUserValidator struct {
	Client  client.Client
	decoder admission.Decoder
}

// SetupWithManager sets up the webhook with the Manager.
func (v *OpenSearchUserValidator) SetupWithManager(mgr ctrl.Manager) error {
	v.Client = mgr.GetClient()
	v.decoder = admission.NewDecoder(mgr.GetScheme())
	return ctrl.NewWebhookManagedBy(mgr).
		For(&opensearchv1.OpensearchUser{}).
		WithValidator(v).
		Complete()
}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (v *OpenSearchUserValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	user := obj.(*opensearchv1.OpensearchUser)

	// Validate that the OpenSearch cluster reference exists
	if err := v.validateClusterReference(ctx, user); err != nil {
		return nil, err
	}

	// Validate that password reference is provided
	if err := v.validatePasswordReference(user); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (v *OpenSearchUserValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldUser := oldObj.(*opensearchv1.OpensearchUser)
	newUser := newObj.(*opensearchv1.OpensearchUser)

	// Validate that the OpenSearch cluster reference hasn't changed
	if err := v.validateClusterReferenceUnchanged(oldUser, newUser); err != nil {
		return nil, err
	}

	// Validate that password reference is provided
	if err := v.validatePasswordReference(newUser); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (v *OpenSearchUserValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	// No validation needed for deletion
	return nil, nil
}

// validateClusterReference validates that the referenced OpenSearch cluster exists and allows the namespace of the resource
func (v *OpenSearchUserValidator) validateClusterReference(ctx context.Context, user *opensearchv1.OpensearchUser) error {
	return validateClusterReference(ctx, v.Client, user.Spec.OpensearchRef, user.Namespace)
}

// validateClusterReferenceUnchanged validates that the cluster reference hasn't changed
func (v *OpenSearchUserValidator) validateClusterReferenceUnchanged(old, new *opensearchv1.OpensearchUser) error {
	if !util.OpensearchClusterRefUnchanged(old.Spec.OpensearchRef, new.Spec.OpensearchRef, new.Namespace) {
		return fmt.Errorf("cannot change the cluster a user refers to")
	}
	return nil
}

// validatePasswordReference validates that password reference is provided
func (v *OpenSearchUserValidator) validatePasswordReference(user *opensearchv1.OpensearchUser) error {
	if user.Spec.PasswordFrom.Name == "" {
		return fmt.Errorf("passwordFrom.name must be specified")
	}
	if user.Spec.PasswordFrom.Key == "" {
		return fmt.Errorf("passwordFrom.key must be specified")
	}
	return nil
}
