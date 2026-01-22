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

package prerender

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/pkg/errors"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chartutil"
)

type execRender struct {
	binaryPath string
	args       []string
}

// NewExec returns a PreRenderer implementation that calls the provided binary.
// It returns an error if the binary cannot be found. If the path does not
// contain any separators, it will search in $PATH, otherwise it will resolve
// any relative paths to a fully qualified path
func NewExec(binaryPath string, args ...string) (PreRenderer, error) {
	fullPath, err := getFullPath(binaryPath)
	if err != nil {
		return nil, err
	}
	return &execRender{fullPath, args}, nil
}

// Run the configured binary for the pre render
func (p *execRender) Run(ch *chart.Chart, values chartutil.Values) (*chart.Chart, chartutil.Values, error) {
	// Serialize chart and values to JSON
	inputJSON, err := serializeChart(ch, values, p.args)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to serialize chart for pre-renderer")
	}

	cmd := exec.Command(p.binaryPath, p.args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}

	var preRendered = &bytes.Buffer{}
	var stderr = &bytes.Buffer{}
	cmd.Stdout = preRendered
	// Use MultiWriter to both capture stderr for error messages and output to actual stderr
	cmd.Stderr = io.MultiWriter(stderr, os.Stderr)

	go func() {
		defer stdin.Close()
		io.Copy(stdin, bytes.NewReader(inputJSON))
	}()

	err = cmd.Run()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error while running command %s. error output:\n%s", p.binaryPath, stderr.String())
	}

	// Deserialize the output
	modifiedChart, modifiedValues, err := deserializeChart(ch, preRendered.Bytes())
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to deserialize pre-renderer output")
	}

	return modifiedChart, modifiedValues, nil
}

// getFullPath returns the full filepath to the binary to execute. If the path
// does not contain any separators, it will search in $PATH, otherwise it will
// resolve any relative paths to a fully qualified path
func getFullPath(binaryPath string) (string, error) {
	// Now check for the binary using the given path or check if it exists in
	// the path and is executable
	checkedPath, err := exec.LookPath(binaryPath)
	if err != nil {
		return "", errors.Wrapf(err, "unable to find binary at %s", binaryPath)
	}

	return filepath.Abs(checkedPath)
}
