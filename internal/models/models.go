package models

type SchemaObject struct {
	Name    string
	Type    string
	Schema  string
	Content string
}

type DiffResult struct {
	Object           SchemaObject
	Exists           bool
	HasDifferences   bool
	DifferenceScript string
}
