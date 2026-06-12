// © 2022 Nokia.
//
// This code is a Contribution to the gNMIc project (“Work”) made under the Google Software Grant and Corporate Contributor License Agreement (“CLA”) and governed by the Apache License 2.0.
// No other rights or licenses in or to any of Nokia’s intellectual property are granted for any other purpose.
// This code is provided on an “as is” basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package gtemplate

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"text/template"
)

func TestDict(t *testing.T) {
	tests := []struct {
		name    string
		args    []any
		want    map[string]any
		wantErr bool
	}{
		{
			name: "empty",
			args: nil,
			want: map[string]any{},
		},
		{
			name: "pairs",
			args: []any{"a", 1, "b", "two"},
			want: map[string]any{"a": 1, "b": "two"},
		},
		{
			name:    "odd number of args",
			args:    []any{"a", 1, "b"},
			wantErr: true,
		},
		{
			name:    "non-string key",
			args:    []any{1, "a"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dict(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("dict() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("dict() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name string
		maps []map[string]any
		want map[string]any
	}{
		{
			name: "empty",
			maps: nil,
			want: map[string]any{},
		},
		{
			name: "disjoint keys",
			maps: []map[string]any{{"a": 1}, {"b": 2}},
			want: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "earlier map takes precedence",
			maps: []map[string]any{{"a": 1}, {"a": 2, "b": 3}},
			want: map[string]any{"a": 1, "b": 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := merge(tt.maps...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestList(t *testing.T) {
	got := list("a", 1, true)
	want := []any{"a", 1, true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("list() = %v, want %v", got, want)
	}
}

// TestLoaderTemplatePattern exercises the accumulate-and-marshal pattern used
// by loader templates: build entries with dict/list, accumulate with merge,
// render with fromJSON.
func TestLoaderTemplatePattern(t *testing.T) {
	const tpl = `{{- $result := dict -}}
{{- range $i, $name := list "target1" "target2" -}}
{{- $entry := dict "address" (printf "%s:57400" $name) "subscriptions" (list "sub1" "sub2") -}}
{{- $result = merge $result (dict $name $entry) -}}
{{- end -}}
{{ fromJSON $result }}`

	tmpl, err := template.New("loader").Funcs(NewTemplateEngine().CreateFuncs()).Parse(tpl)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, buf.String())
	}
	want := map[string]any{
		"target1": map[string]any{
			"address":       "target1:57400",
			"subscriptions": []any{"sub1", "sub2"},
		},
		"target2": map[string]any{
			"address":       "target2:57400",
			"subscriptions": []any{"sub1", "sub2"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rendered config = %v, want %v", got, want)
	}
}

func TestDictErrorFailsTemplateExecution(t *testing.T) {
	tmpl, err := template.New("bad").Funcs(NewTemplateEngine().CreateFuncs()).Parse(`{{ dict "a" }}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := tmpl.Execute(&bytes.Buffer{}, nil); err == nil {
		t.Fatal("expected execution error for odd dict arguments, got nil")
	}
}
