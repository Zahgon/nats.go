package builtin

type GobEncoder struct {
}

func (ge *GobEncoder) Encode(subject string, v any) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (ge *GobEncoder) Decode(subject string, data []byte, vPtr any) (err error) {
	_ = "STUB: not implemented"
	return nil
}
