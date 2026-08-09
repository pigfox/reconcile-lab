package reconcilelab

import (
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestStoreCreateAssignsAVersion(t *testing.T) {
	s := NewStore()

	got, err := s.Create(NewDeployment("web", targetReplicas))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got.ResourceVersion == "" {
		t.Error("the created object carries no resourceVersion — this is the whole mechanism of the demo, " +
			"and client-go's fake tracker returning an empty one is exactly why this store exists")
	}
	if got.Name != "web" {
		t.Errorf("Name = %q, want %q", got.Name, "web")
	}
	if Replicas(got) != targetReplicas {
		t.Errorf("Replicas = %d, want the seed value %d", Replicas(got), targetReplicas)
	}
	if s.Len() != 1 {
		t.Errorf("Len = %d, want 1", s.Len())
	}
}

func TestStoreCreateRefusesADuplicate(t *testing.T) {
	s := NewStore()
	if _, err := s.Create(NewDeployment("web", targetReplicas)); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := s.Create(NewDeployment("web", targetReplicas))
	if !apierrors.IsAlreadyExists(err) {
		t.Errorf("second Create returned %v, want AlreadyExists — a seeding bug that silently replaced an "+
			"object would surface much later as a divergence nobody could account for", err)
	}
}

func TestStoreGetReportsNotFound(t *testing.T) {
	s := NewStore()

	_, err := s.Get("absent")
	if !apierrors.IsNotFound(err) {
		t.Errorf("Get of an absent object returned %v, want NotFound", err)
	}
}

// A READ HANDS BACK A COPY, and this is not a precaution. If a caller could
// mutate the stored object directly it would be writing with no version check at
// all, and every conflict in the simulation would become unreachable.
func TestStoreGetReturnsACopy(t *testing.T) {
	s := NewStore()
	if _, err := s.Create(NewDeployment("web", targetReplicas)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	first, err := s.Get("web")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	SetReplicas(first, 99)
	first.Labels["tampered"] = "yes"

	second, err := s.Get("web")
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if Replicas(second) != targetReplicas {
		t.Errorf("Replicas = %d after a caller mutated its copy, want %d", Replicas(second), targetReplicas)
	}
	if _, tampered := second.Labels["tampered"]; tampered {
		t.Error("a caller's mutation of a nested map reached the stored object — the copy is not deep")
	}
}

func TestStoreUpdateAcceptsTheCurrentVersion(t *testing.T) {
	s := NewStore()
	seed, err := s.Create(NewDeployment("web", targetReplicas))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	seed.Labels["stage"] = "one"
	next, err := s.Update(seed)
	if err != nil {
		t.Fatalf("Update with the current version: %v", err)
	}
	if next.ResourceVersion == seed.ResourceVersion {
		t.Error("the resourceVersion did not move on a landed write, so a stale write could never be detected")
	}
	if next.Labels["stage"] != "one" {
		t.Error("the update did not store what it was given")
	}
}

// THE MECHANISM. Two readers, both holding the same version; the first write
// lands and the second is refused with a real Conflict — the error
// apierrors.IsConflict recognizes, not a sentinel of our own.
func TestStoreUpdateRefusesAStaleVersion(t *testing.T) {
	s := NewStore()
	if _, err := s.Create(NewDeployment("web", targetReplicas)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	first, err := s.Get("web")
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	second, err := s.Get("web")
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}

	SetReplicas(first, 5)
	if _, err := s.Update(first); err != nil {
		t.Fatalf("the first write should have landed: %v", err)
	}

	SetReplicas(second, 9)
	_, err = s.Update(second)
	if !apierrors.IsConflict(err) {
		t.Fatalf("the stale write returned %v, want a Conflict", err)
	}

	// And the loser's value must not be in the store.
	current, err := s.Get("web")
	if err != nil {
		t.Fatalf("Get after the conflict: %v", err)
	}
	if Replicas(current) != 5 {
		t.Errorf("Replicas = %d, want 5 — the refused write changed the object anyway", Replicas(current))
	}
}

// RE-READ AND RETRY IS THE PATH OUT, and it has to actually work or the retry
// policies would be comparing three ways of failing.
func TestStoreRetryAfterConflictLands(t *testing.T) {
	s := NewStore()
	if _, err := s.Create(NewDeployment("web", targetReplicas)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	stale, err := s.Get("web")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	winner, err := s.Get("web")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	SetReplicas(winner, 7)
	if _, err := s.Update(winner); err != nil {
		t.Fatalf("Update: %v", err)
	}
	SetReplicas(stale, 9)
	if _, err := s.Update(stale); !apierrors.IsConflict(err) {
		t.Fatalf("expected a conflict, got %v", err)
	}

	fresh, err := s.Get("web")
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	SetReplicas(fresh, 9)
	if _, err := s.Update(fresh); err != nil {
		t.Errorf("the retry after a re-read did not land: %v", err)
	}
}

func TestStoreUpdateReportsNotFound(t *testing.T) {
	s := NewStore()

	_, err := s.Update(NewDeployment("absent", targetReplicas))
	if !apierrors.IsNotFound(err) {
		t.Errorf("Update of an absent object returned %v, want NotFound", err)
	}
}

// AN EMPTY VERSION CANNOT SLIP PAST THE CHECK. A struct nobody filled in carries
// the zero value of the field, and if the counter started at zero that value
// could match a stored version by accident.
func TestStoreUpdateRefusesAnUnsetVersion(t *testing.T) {
	s := NewStore()
	if _, err := s.Create(NewDeployment("web", targetReplicas)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	bare := NewDeployment("web", targetReplicas)
	if bare.ResourceVersion != "" {
		t.Fatalf("the fixture already carries a version (%q), so this proves nothing", bare.ResourceVersion)
	}
	if _, err := s.Update(bare); !apierrors.IsConflict(err) {
		t.Errorf("a write carrying no version returned %v, want a Conflict", err)
	}
}

func TestReplicasHandlesAnUnsetPointer(t *testing.T) {
	d := NewDeployment("web", targetReplicas)
	d.Spec.Replicas = nil

	if got := Replicas(d); got != unsetReplicas {
		t.Errorf("Replicas of an object with a nil pointer = %d, want the sentinel %d", got, unsetReplicas)
	}
	if got := Replicas(nil); got != unsetReplicas {
		t.Errorf("Replicas(nil) = %d, want the sentinel %d", got, unsetReplicas)
	}
	if unsetReplicas == targetReplicas {
		t.Error("the unset sentinel equals the target, so an object with no replica count would read as converged")
	}
}

func TestSetReplicasAllocatesItsOwnPointer(t *testing.T) {
	d := NewDeployment("web", targetReplicas)
	SetReplicas(d, 11)
	if got := Replicas(d); got != 11 {
		t.Errorf("Replicas = %d, want 11", got)
	}

	// The pointer must not be shared with the caller's variable. The mutation
	// below is the whole test, so it is read back as well as written — an
	// assignment nothing observes is one a linter is right to call ineffectual,
	// and asserting it also proves the fixture did what it claims.
	n := int32(1)
	SetReplicas(d, n)
	n = 2
	if n != 2 {
		t.Fatalf("the fixture did not change the caller's variable (n = %d), so it proves nothing", n)
	}
	if got := Replicas(d); got != 1 {
		t.Errorf("Replicas = %d after the caller's variable changed, want 1 — SetReplicas kept the caller's pointer", got)
	}
}

// THE SEED OBJECT IS A VALID DEPLOYMENT, not a name with a number attached. The
// simulation reads none of this, which is exactly why it is worth pinning: the
// object a reader inspects should be the shape they would recognize.
func TestNewDeploymentIsAWellFormedObject(t *testing.T) {
	d := NewDeployment("web", targetReplicas)

	if d.Namespace != deploymentNamespace {
		t.Errorf("Namespace = %q, want %q", d.Namespace, deploymentNamespace)
	}
	if d.Spec.Selector == nil {
		t.Fatal("the deployment has no selector")
	}
	for k, v := range d.Spec.Selector.MatchLabels {
		if d.Spec.Template.Labels[k] != v {
			t.Errorf("the selector requires %s=%s but the pod template does not carry it — "+
				"a real API server would reject this object", k, v)
		}
	}
	if len(d.Spec.Template.Spec.Containers) == 0 {
		t.Error("the pod template has no containers")
	}
}
