package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// Validator loads a JSON Schema Draft 2020-12 document from disk.
type Validator struct {
	path   string
	schema *jsonschema.Schema
}

func Load(path string) (*Validator, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("abs schema path: %w", err)
	}
	c := jsonschema.NewCompiler()

	schema, err := c.Compile("file://" + abs)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", abs, err)
	}
	return &Validator{path: abs, schema: schema}, nil
}

// Validate unmarshals payload as generic JSON then validates against the compiled schema.
func (v *Validator) ValidateJSON(payload []byte) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("json decode instance: %w", err)
	}
	if err := v.schema.Validate(doc); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}
	return nil
}

// ValidateInterface validates an already decoded structure by re-encoding to JSON first.
func (v *Validator) ValidateInterface(inst any) error {
	bs, err := json.Marshal(inst)
	if err != nil {
		return fmt.Errorf("json encode instance: %w", err)
	}
	return v.ValidateJSON(bs)
}
