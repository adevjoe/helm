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

package driver // import "helm.sh/helm/v3/pkg/storage/driver"

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"

	rspb "helm.sh/helm/v3/pkg/release"
	v1 "k8s.io/api/core/v1"
)

var b64 = base64.StdEncoding

var magicGzip = []byte{0x1f, 0x8b, 0x08}

// encodeRelease encodes a release returning a base64 encoded
// gzipped string representation, or error.
func encodeRelease(rls *rspb.Release) (string, error) {
	b, err := json.Marshal(rls)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return "", err
	}
	if _, err = w.Write(b); err != nil {
		return "", err
	}
	w.Close()

	return b64.EncodeToString(buf.Bytes()), nil
}

// decodeRelease decodes the bytes of data into a release
// type. Data must contain a base64 encoded gzipped string of a
// valid release, otherwise an error is returned.
func decodeRelease(data string) (*rspb.Release, error) {
	// base64 decode string
	b, err := b64.DecodeString(data)
	if err != nil {
		return nil, err
	}

	// For backwards compatibility with releases that were stored before
	// compression was introduced we skip decompression if the
	// gzip magic header is not found
	if bytes.Equal(b[0:3], magicGzip) {
		r, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		b2, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
		b = b2
	}

	var rls rspb.Release
	// unmarshal release object bytes
	if err := json.Unmarshal(b, &rls); err != nil {
		return nil, err
	}
	return &rls, nil
}

func (secrets *Secrets) cacheDecodeReleases(list *v1.SecretList, filter func(*rspb.Release) bool) []*rspb.Release {
	var results []*rspb.Release
	resultChan := make(chan *rspb.Release, len(list.Items))

	wg := sync.WaitGroup{}
	wg.Add(len(list.Items))
	for _, item := range list.Items {
		go func(item v1.Secret) {
			defer wg.Done()
			cacheKey := fmt.Sprintf("%s-%s-%s", item.GetUID(), item.GetNamespace(), item.GetName())
			if i, ok := secrets.cache.Get(cacheKey); ok {
				if ii, ok := i.(*rspb.Release); ok {
					resultChan <- ii.Deepcopy()
					return
				}
			}
			rls, err := decodeRelease(string(item.Data["release"]))
			if err != nil {
				secrets.Log("list: failed to decode release: %v: %s", item, err)
				return
			}

			rls.Labels = item.ObjectMeta.Labels
			if rls.Info.Status == rspb.StatusSuperseded || rls.Info.Status == rspb.StatusFailed {
				secrets.cache.Set(cacheKey, rls, int64(len(item.Data["release"])))
			}
			resultChan <- rls
		}(item)
	}
	wg.Wait()
	close(resultChan)
	for k := range resultChan {
		if filter == nil {
			results = append(results, k)
			continue
		}
		if filter(k) {
			results = append(results, k)
		}
	}
	return results
}
