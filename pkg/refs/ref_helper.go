package refs

// NewPointer is a generic function to create a pointer to any type.
func NewPointer[T any](value T) *T {
	return &value
}

// DerefPointer is a generic function to dereference a pointer with a default-value fallback.
func DerefPointer[T any](ptr *T, defaults ...T) T {
	if ptr != nil {
		return *ptr
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	var empty T
	return empty
}
