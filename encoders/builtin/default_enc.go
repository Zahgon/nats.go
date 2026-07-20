package builtin

type DefaultEncoder struct {
}

var trueB = []byte("true")
var falseB = []byte("false")
var nilB = []byte("")

func (je *DefaultEncoder) Encode(subject string, v any) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (je *DefaultEncoder) Decode(subject string, data []byte, vPtr any) error {
	_ = "STUB: not implemented"
	return nil
}
