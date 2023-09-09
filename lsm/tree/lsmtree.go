package tree

type LSMTree struct {
}

func (table *LSMTree) Put(key, value string) DbError {
	return NewDbError("todo", InternalError)
}

func (table *LSMTree) Delete(key string) DbError {
	return NewDbError("todo", InternalError)
}

func (table *LSMTree) Get(key string) (value string, err DbError) {
	return "", NewDbError("todo", InternalError)
}
