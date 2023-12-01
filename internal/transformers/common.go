package transformers

import (
	"math/rand"
	"time"
)

type Transformer interface {
	Transform(value interface{}) interface{}
}

type EmailTransformer struct{}

func (et EmailTransformer) Transform(value interface{}) interface{} {
	return "fake@example.org"
}

type NilTransformer struct{}

func (nt NilTransformer) Transform(value interface{}) interface{} {
	return nil
}

type RandomEmailTransformer struct{}

func (ret RandomEmailTransformer) Transform(value interface{}) interface{} {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	randBytes := make([]byte, 10)
	for i := range randBytes {
		// Generate a random number between 0 and 25 (for letters a-z)
		randBytes[i] = byte('a' + rand.Intn(26))
	}

	return string(randBytes) + "@example.org"
}

type EmptyJSONTransformer struct{}

func (ejt EmptyJSONTransformer) Transform(value interface{}) interface{} {
	return "{}"
}
