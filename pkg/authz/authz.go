// Package authz defines who may do what. Authentication (who are you?) lives in
// auth-svc and pkg/jwt; this package answers the next question: is that caller
// allowed to perform this action?
package authz

// Role is a caller's privilege level, carried in the access token.
type Role string

const (
	RoleUser      Role = "user"
	RoleOrganizer Role = "organizer"
	RoleAdmin     Role = "admin"
)

// The roles form a ladder: an admin can do everything an organizer can, and an
// organizer everything a user can.
var rank = map[Role]int{
	RoleUser:      1,
	RoleOrganizer: 2,
	RoleAdmin:     3,
}

// Valid reports whether r is one of the known roles.
func (r Role) Valid() bool {
	_, ok := rank[r]
	return ok
}

// AtLeast reports whether r grants at least the privileges of min.
//
// It fails closed: an unknown role, such as an empty or forged claim, grants
// nothing, and so does asking for an unknown minimum. Otherwise a typo in a
// route's requirement would quietly open it to everyone.
func (r Role) AtLeast(min Role) bool {
	have, ok := rank[r]
	if !ok {
		return false
	}
	need, ok := rank[min]
	if !ok {
		return false
	}
	return have >= need
}
