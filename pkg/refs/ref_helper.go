package refs

// NewPointer is a generic function to create a pointer to any type.
func NewPointer[T any](value T) *T {
	return &value
}
