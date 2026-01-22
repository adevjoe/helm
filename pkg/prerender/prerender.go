/*
Copyright The Helm Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package prerender contains an interface that can be implemented for custom
// pre-renderers and an exec implementation that can be used for arbitrary
// binaries and scripts
package prerender

import (
	"encoding/base64"
	"encoding/json"

	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chartutil"
)

// PreRenderer is an interface that can be implemented for custom pre-renderers
type PreRenderer interface {
	// Run expects a chart and values before rendering. It
	// expects the modified chart and values to be returned or an
	// error if there was an issue or failure while running the pre render step
	Run(ch *chart.Chart, values chartutil.Values) (modifiedChart *chart.Chart, modifiedValues chartutil.Values, err error)
}

// templateInfo represents a template file for JSON serialization
type templateInfo struct {
	Name string `json:"name"`
	Data string `json:"data"` // base64 encoded
}

// preRenderInput represents the input to a pre-renderer
type preRenderInput struct {
	ChartTemplates []templateInfo              `json:"chartTemplates"`
	Values         map[string]interface{}      `json:"values"`
	ChartMetadata  *chart.Metadata              `json:"chartMetadata"`
	ExtraArgs      []string                    `json:"extraArgs"`
}

// preRenderOutput represents the output from a pre-renderer
type preRenderOutput struct {
	ChartTemplates []templateInfo         `json:"chartTemplates"`
	Values         map[string]interface{} `json:"values"`
}

// serializeChart serializes a chart to JSON format for pre-renderer input
func serializeChart(ch *chart.Chart, values chartutil.Values, extraArgs []string) ([]byte, error) {
	// Serialize chart templates
	templates := make([]templateInfo, 0, len(ch.Templates))
	for _, t := range ch.Templates {
		if t == nil {
			continue
		}
		templates = append(templates, templateInfo{
			Name: t.Name,
			Data: base64.StdEncoding.EncodeToString(t.Data),
		})
	}

	input := preRenderInput{
		ChartTemplates: templates,
		Values:         values,
		ChartMetadata:  ch.Metadata,
		ExtraArgs:      extraArgs,
	}

	return json.Marshal(input)
}

// deserializeChart deserializes JSON output from pre-renderer back to chart and values
func deserializeChart(ch *chart.Chart, outputJSON []byte) (*chart.Chart, chartutil.Values, error) {
	var output preRenderOutput
	if err := json.Unmarshal(outputJSON, &output); err != nil {
		return nil, nil, err
	}

	// Update chart templates
	ch.Templates = make([]*chart.File, 0, len(output.ChartTemplates))
	for _, t := range output.ChartTemplates {
		data, err := base64.StdEncoding.DecodeString(t.Data)
		if err != nil {
			return nil, nil, err
		}
		ch.Templates = append(ch.Templates, &chart.File{
			Name: t.Name,
			Data: data,
		})
	}

	// Convert values back to chartutil.Values
	modifiedValues := chartutil.Values(output.Values)

	return ch, modifiedValues, nil
}
