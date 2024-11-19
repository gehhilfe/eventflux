package api

type invalidFieldError struct {
	field string
}

func (e *invalidFieldError) Error() string {
	return "Invalid field: " + e.field
}
