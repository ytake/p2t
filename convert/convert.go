package convert

type ToDDLTransformer interface {
	// Transform transforms the table.
	Transform() string
}

// To is a function for converting parquet schema to ddl.
func To(t ToDDLTransformer) []byte {
	return []byte(t.Transform())
}
