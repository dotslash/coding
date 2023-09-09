package tree

import "errors"

type LSMTree struct {
}

func (table *LSMTree) Put(key, value string) DbError {
	return NewInternalError(errors.New("todo"))
}

func (table *LSMTree) Delete(key string) DbError {
	return NewInternalError(errors.New("todo"))
}

func (table *LSMTree) Get(key string) (value string, err DbError) {
	return "", NewInternalError(errors.New("todo"))
}
