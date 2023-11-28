package transformers

type Transformer interface {
	Transform(value interface{}) interface{}
}

type EmailTransformer struct{}

func (et EmailTransformer) Transform(value interface{}) interface{} {
	return "fake@example.org"
}
