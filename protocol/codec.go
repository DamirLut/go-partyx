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

// Call adapts a typed handler to the raw payload protocol: the payload is
// decoded into Req (an empty payload yields a zero Req), Validate() is
// called when Req implements it (failures map to a 400 error), fn runs, and
// the response is returned for encoding. A typed-nil response becomes a nil
// Marshaler (empty payload).
func Call[Req any, Resp any, PReq interface {
	*Req
	Unmarshaler
}, PResp interface {
	*Resp
	Marshaler
}](payload []byte, fn func(req PReq) (PResp, error)) (Marshaler, error) {
	req, err := Decode[Req, PReq](payload)
	if err != nil {
		return nil, NewError(400, "invalid payload")
	}
	if v, ok := any(PReq(req)).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return nil, NewError(400, err.Error())
		}
	}
	resp, err := fn(PReq(req))
	if err != nil {
		return nil, err
	}
	// Compare via the core type: a typed-nil pointer boxed in an
	// interface is not == nil, but the client expects an empty payload.
	if (*Resp)(resp) == nil {
		return nil, nil
	}
	return resp, nil
}
