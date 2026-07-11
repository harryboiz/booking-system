package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

func Load(path string, target any) error {
	values, err := loadValues(path, make(map[string]bool))
	if err != nil {
		return err
	}
	data, err := yaml.Marshal(values)
	if err != nil {
		return fmt.Errorf("merge config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(data, target); err != nil {
		return fmt.Errorf("parse config %q: %w", path, err)
	}
	return nil
}

func loadValues(path string, loading map[string]bool) (map[string]any, error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path %q: %w", path, err)
	}
	if loading[absolutePath] {
		return nil, fmt.Errorf("config include cycle detected at %q", path)
	}
	loading[absolutePath] = true
	defer delete(loading, absolutePath)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var document struct {
		Includes []string `yaml:"includes"`
	}
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	var current map[string]any
	if err := yaml.Unmarshal(data, &current); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	delete(current, "includes")

	merged := make(map[string]any)
	for _, include := range document.Includes {
		includePath := include
		if !filepath.IsAbs(includePath) {
			includePath = filepath.Join(filepath.Dir(path), includePath)
		}
		included, err := loadValues(includePath, loading)
		if err != nil {
			return nil, err
		}
		mergeValues(merged, included)
	}
	mergeValues(merged, current)
	return merged, nil
}

func mergeValues(destination, source map[string]any) {
	for key, sourceValue := range source {
		sourceMap, sourceIsMap := sourceValue.(map[string]any)
		destinationMap, destinationIsMap := destination[key].(map[string]any)
		if sourceIsMap && destinationIsMap {
			mergeValues(destinationMap, sourceMap)
			continue
		}
		destination[key] = sourceValue
	}
}
