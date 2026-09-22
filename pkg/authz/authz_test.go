package authz

import "testing"

func TestRole_AtLeast(t *testing.T) {
	cases := []struct {
		have, need Role
		want       bool
	}{
		{RoleUser, RoleUser, true},
		{RoleUser, RoleOrganizer, false},
		{RoleUser, RoleAdmin, false},
		{RoleOrganizer, RoleUser, true},
		{RoleOrganizer, RoleOrganizer, true},
		{RoleOrganizer, RoleAdmin, false},
		{RoleAdmin, RoleUser, true},
		{RoleAdmin, RoleOrganizer, true},
		{RoleAdmin, RoleAdmin, true},
	}
	for _, tc := range cases {
		if got := tc.have.AtLeast(tc.need); got != tc.want {
			t.Errorf("%q.AtLeast(%q) = %v, want %v", tc.have, tc.need, got, tc.want)
		}
	}
}

func TestRole_UnknownRolesGrantNothing(t *testing.T) {
	for _, bogus := range []Role{"", "root", "ADMIN", "Admin", " admin"} {
		for _, need := range []Role{RoleUser, RoleOrganizer, RoleAdmin} {
			if bogus.AtLeast(need) {
				t.Errorf("unknown role %q was granted %q access", bogus, need)
			}
		}
	}
}

// A mistyped requirement must lock a route, never open it.
func TestRole_UnknownRequirementDeniesEveryone(t *testing.T) {
	for _, have := range []Role{RoleUser, RoleOrganizer, RoleAdmin} {
		if have.AtLeast("organiser") {
			t.Errorf("%q passed a requirement that names no real role", have)
		}
	}
}

func TestRole_Valid(t *testing.T) {
	for _, r := range []Role{RoleUser, RoleOrganizer, RoleAdmin} {
		if !r.Valid() {
			t.Errorf("%q should be valid", r)
		}
	}
	for _, r := range []Role{"", "root", "Admin"} {
		if r.Valid() {
			t.Errorf("%q should not be valid", r)
		}
	}
}
