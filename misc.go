package streamfleet

import (
	"fmt"
	"github.com/google/uuid"
)

// MustUuidV7 returns a new UUIDv7 string.
// If there are errors in creating it (which could only ever happen because of an OS error related to PRNG), the function panics.
func MustUuidV7() string {
	id, err := uuid.NewV7()
	if err != nil {
		panic(fmt.Errorf("streamfleet: failed to generate a new UUIDv7 value. this should never happen; your operating system may have failed to generate random data or acquire entropy. error: %w", err))
	}

	return id.String()
}
