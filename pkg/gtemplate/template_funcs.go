// © 2022 Nokia.
//
// This code is a Contribution to the gNMIc project (“Work”) made under the Google Software Grant and Corporate Contributor License Agreement (“CLA”) and governed by the Apache License 2.0.
// No other rights or licenses in or to any of Nokia’s intellectual property are granted for any other purpose.
// This code is provided on an “as is” basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package gtemplate

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"text/template"

	"gopkg.in/yaml.v2"
)

type templateEngine interface {
	CreateFuncs() template.FuncMap
}

func NewTemplateEngine() templateEngine {
	return &gmplt{}
}

type gmplt struct{}

func (*gmplt) CreateFuncs() template.FuncMap {
	return template.FuncMap{
		"fromJSON":   fromJSON,
		"toJSON":     toJSON,
		"fromYAML":   fromYAML,
		"toYAML":     toYAML,
		"split":      split,
		"join":       join,
		"replace":    replace,
		"trimPrefix": trimPrefix,
		"trimSuffix": trimSuffix,
		"toTitle":    toTitle,
		"toLower":    toLower,
		"toUpper":    toUpper,
		"pathBase":   pathBase,
		"dict":       dict,
		"merge":      merge,
		"list":       list,
	}
}

func fromJSON(v any) string {
	a, _ := json.Marshal(v)
	return string(a)
}

func toJSON(v string) any {
	var result any
	json.Unmarshal([]byte(v), &result)
	return result
}

func fromYAML(v any) string {
	a, _ := yaml.Marshal(v)
	return string(a)
}

func toYAML(v string) any {
	var result any
	yaml.Unmarshal([]byte(v), &result)
	return result
}

func split(v string, sep string) []string {
	return strings.Split(v, sep)
}

func join(v []string, sep string) string {
	return strings.Join(v, sep)
}

func replace(v string, old string, new string) string {
	return strings.ReplaceAll(v, old, new)
}

func trimPrefix(v string, prefix string) string {
	return strings.TrimPrefix(v, prefix)
}

func trimSuffix(v string, suffix string) string {
	return strings.TrimSuffix(v, suffix)
}

func toTitle(v string) string {
	return strings.Title(v)
}

func toLower(v string) string {
	return strings.ToLower(v)
}

func toUpper(v string) string {
	return strings.ToUpper(v)
}

func pathBase(v string) string {
	return filepath.Base(v)
}

func dict(pairs ...any) (map[string]any, error) {
	if len(pairs)%2 != 0 {
		return nil, fmt.Errorf("dict requires an even number of arguments, got %d", len(pairs))
	}
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		k, ok := pairs[i].(string)
		if !ok {
			return nil, fmt.Errorf("dict keys must be strings, got %T at position %d", pairs[i], i)
		}
		m[k] = pairs[i+1]
	}
	return m, nil
}

// merge performs a shallow merge of maps, values in earlier maps take
// precedence over later ones.
func merge(maps ...map[string]any) map[string]any {
	result := make(map[string]any)
	for _, m := range maps {
		for k, v := range m {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}
	return result
}

func list(items ...any) []any {
	return items
}
