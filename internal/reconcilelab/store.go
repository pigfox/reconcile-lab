package reconcilelab

import (
	"errors"
	"strconv"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Store is a versioned object store with compare-and-swap writes, holding real
// k8s.io/api appsv1.Deployment values.
//
// IT IS NOT A FAKE CLIENT AND DELIBERATELY SO. PF-S193b measured
// k8s.io/client-go/kubernetes/fake and found its object tracker assigns no
// resourceVersion at all — empty on read, still empty after an update — and
// accepts a write carrying a stale version without complaint. Every conflict on
// that substrate would have to be injected by a reactor on a schedule the demo
// chose, and a conflict the page arranged is not a measurement of anything. The
// contention this demo reports has to arise from two writers racing, so the
// store has to actually enforce the version.
//
// WHAT IS REAL HERE AND WHAT IS NOT, stated plainly because the page makes a
// claim about it. The OBJECT is real: appsv1.Deployment, metav1.ObjectMeta, the
// generated DeepCopy, and the k8s.io/apimachinery error constructors, so a
// rejected write comes back as an error apierrors.IsConflict recognizes. The
// SERVER is not: there is no etcd, no admission, no watch cache and no network.
// This is the write path of an API server and nothing else of one.
//
// RESOURCE VERSIONS ARE COMPARED FOR EQUALITY ONLY, never ordering. Kubernetes
// documents resourceVersion as opaque, and a store that let callers do
// arithmetic on it would teach a reader the one thing about it that is wrong.
// The counter behind it is an implementation detail of this file.
type Store struct {
	mu   sync.Mutex
	objs map[string]*appsv1.Deployment
	next uint64
}

// deploymentsResource names the group and resource the API errors are raised
// against, so a conflict reads the way a real one does.
var deploymentsResource = schema.GroupResource{Group: "apps", Resource: "deployments"}

// errObjectModified is the cause carried by every conflict this store raises. It
// is the wording the API server uses, so the rendered message is the one an
// operator would recognize from a terminal.
var errObjectModified = errors.New("object was modified")

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{objs: make(map[string]*appsv1.Deployment), next: firstResourceVersion}
}

// bumpLocked advances the version counter and returns the new value as the
// opaque string a resourceVersion is. The caller must hold s.mu.
func (s *Store) bumpLocked() string {
	s.next++
	return strconv.FormatUint(s.next, 10)
}

// Create adds an object. It reports AlreadyExists rather than overwriting,
// because a seeding bug that silently replaced an object would show up much
// later as a divergence nobody could account for.
func (s *Store) Create(d *appsv1.Deployment) (*appsv1.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.objs[d.Name]; exists {
		return nil, apierrors.NewAlreadyExists(deploymentsResource, d.Name)
	}

	stored := d.DeepCopy()
	stored.ResourceVersion = s.bumpLocked()
	s.objs[d.Name] = stored
	return stored.DeepCopy(), nil
}

// Get returns a DEEP COPY of the stored object.
//
// The copy is the point rather than a precaution: a caller that mutated the
// stored object directly would be writing without a version check, which is the
// entire failure mode this demo exists to show. Handing out the pointer would
// make every conflict in the simulation unreachable.
func (s *Store) Get(name string) (*appsv1.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.objs[name]
	if !ok {
		return nil, apierrors.NewNotFound(deploymentsResource, name)
	}
	return stored.DeepCopy(), nil
}

// Update writes d back if and only if d carries the version currently stored.
//
// This is the whole mechanism of the demo. A caller reads, thinks, and writes;
// if anything else landed a write in between, the version it is carrying is no
// longer current and the write is refused with a real Conflict. Nothing here
// merges, retries or resolves — deciding what to do about a refusal is the
// reconcile loop's job and is the policy this demo varies.
func (s *Store) Update(d *appsv1.Deployment) (*appsv1.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.objs[d.Name]
	if !ok {
		return nil, apierrors.NewNotFound(deploymentsResource, d.Name)
	}
	if d.ResourceVersion != stored.ResourceVersion {
		return nil, apierrors.NewConflict(deploymentsResource, d.Name, errObjectModified)
	}

	next := d.DeepCopy()
	next.ResourceVersion = s.bumpLocked()
	s.objs[d.Name] = next
	return next.DeepCopy(), nil
}

// Len is how many objects the store holds.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objs)
}

// NewDeployment builds one seed object at the given replica count.
//
// The spec is filled in far enough to be a VALID Deployment rather than a
// name with a number attached — a selector that matches the pod template's
// labels, and a container. Nothing in the simulation reads any of it, and that
// is exactly why it is here: the object a reader inspects should be the shape
// they would recognize, not the subset this file happens to touch.
func NewDeployment(name string, replicas int32) *appsv1.Deployment {
	labels := map[string]string{deploymentLabelKey: name}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: deploymentNamespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: containerName, Image: containerImage}},
				},
			},
		},
	}
}

// Replicas reads the replica count off an object.
//
// Spec.Replicas is a *int32 in the real type — nil means "unset, default 1" to
// the API server — so every reader has to make that decision explicitly. This
// package's objects always carry one, and a nil here would be a bug rather than
// a default, so it returns the sentinel instead of dereferencing.
func Replicas(d *appsv1.Deployment) int32 {
	if d == nil || d.Spec.Replicas == nil {
		return unsetReplicas
	}
	return *d.Spec.Replicas
}

// SetReplicas writes the replica count, allocating the pointer the real type
// requires rather than assuming the caller kept one.
func SetReplicas(d *appsv1.Deployment, n int32) {
	v := n
	d.Spec.Replicas = &v
}
