package protocol

// Error is an RPC error carrying a wire code. Handlers return it to control
// the code sent to the client; any other error maps to 500.
type Error struct {
	Code    int
	Message string
}

func NewError(code int, message string) *Error {
	return &Error{Code: code, Message: message}
}

func (e *Error) Error() string {
	return e.Message
}
