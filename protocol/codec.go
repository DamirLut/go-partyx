package protocol

// Marshaler is implemented by every arpack-generated message type
// (pointer receiver). Hand-written types can implement it too.
type Marshaler interface {
	Marshal(buf []byte) []byte
}

// Unmarshaler is implemented by pointers to arpack-generated message types.
type Unmarshaler interface {
	Unmarshal(data []byte) (int, error)
}

const (
	// UserMethodOpBase is the first method opcode available to games;
	// 0-99 is reserved for the framework (see MethodOp).
	UserMethodOpBase = 100
	// UserEventOpBase is the first event opcode available to games;
	// 0-99 is reserved for the framework (see EventOp).
	UserEventOpBase = 100
)

// Encode marshals m into a fresh byte slice. A nil m yields a nil slice,
// which the wire protocol treats as an empty payload.
func Encode(m Marshaler) []byte {
	if m == nil {
		return nil
	}
	return m.Marshal(make([]byte, 0, 64))
}

// Decode unmarshals data into a new T and returns it. An empty payload
// yields a zero T, so methods without a body need no payload type.
func Decode[T any, PT interface {
	*T
	Unmarshaler
}](data []byte) (*T, error) {
	v := new(T)
	if len(data) == 0 {
		return v, nil
	}
	if _, err := PT(v).Unmarshal(data); err != nil {
		return nil, err
	}
	return v, nil
}
