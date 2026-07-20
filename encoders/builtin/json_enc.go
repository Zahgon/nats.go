package builtin

type JsonEncoder struct {
}

func (je *JsonEncoder) Encode(subject string, v any) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (je *JsonEncoder) Decode(subject string, data []byte, vPtr any) (err error) {
	_ = "STUB: not implemented"
	return nil
}
