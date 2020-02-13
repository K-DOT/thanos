package e2ethanos

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/receive"
)

// TODO(bwplotka): Run against multiple?
func DefaultPrometheusImage() string {
	return "quay.io/prometheus/prometheus:v2.16.0"
}

// DefaultImage returns the local docker image to use to run Thanos.
func DefaultImage() string {
	// Get the Thanos image from the THANOS_IMAGE env variable.
	if os.Getenv("THANOS_IMAGE") != "" {
		return os.Getenv("THANOS_IMAGE")
	}

	return "thanos"
}

func NewPrometheusWithSidecar(sharedDir string, name string, config, promImage string) (*e2e.HTTPService, *Service, error) {
	dir := filepath.Join(sharedDir, "data", "prometheus", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "prometheus", name)
	if err := ioutil.WriteFile(filepath.Join(dir, "prometheus.yml"), []byte(config), 0666); err != nil {
		return nil, nil, errors.Wrap(err, "creating prom config failed")
	}

	prom := e2e.NewHTTPService(
		fmt.Sprintf("prometheus-%s", name),
		promImage,
		nil,
		e2e.NewCommandWithoutEntrypoint("prometheus", e2e.BuildArgs(map[string]string{
			"--config.file":                     filepath.Join(container, "prometheus.yml"),
			"--storage.tsdb.path":               container,
			"--storage.tsdb.max-block-duration": "2h",
			"--log.level":                       "info",
			"--web.listen-address":              ":9090",
		})...),
		e2e.NewReadinessProbe(9090, "/-/ready", 200),
		9090,
	)
	sidecar := NewService(
		fmt.Sprintf("sidecar-%s", name),
		DefaultImage(),
		nil,
		e2e.NewCommand("sidecar", e2e.BuildArgs(map[string]string{
			"--debug.name":        fmt.Sprintf("sidecar-%v", name),
			"--grpc-address":      ":9091",
			"--grpc-grace-period": "0s",
			"--http-address":      ":80",
			"--prometheus.url":    prom.HTTPEndpoint(),
			"--tsdb.path":         container,
			"--log.level":         "info",
		})...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
		9091,
	)
	return prom, sidecar, nil
}

func generateFileSD(addresses []string) string {
	conf := "[ { \"targets\": ["
	for index, addr := range addresses {
		conf += fmt.Sprintf("\"%s\"", addr)
		if index+1 < len(addresses) {
			conf += ","
		}
	}
	conf += "] } ]"
	return conf
}

func NewQuerier(sharedDir string, name string, storeAddresses []string, fileSDStoreAddresses []string) (*Service, error) {
	const replicaLabel = "replica"

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":            fmt.Sprintf("querier-%v", name),
		"--grpc-address":          ":9091",
		"--grpc-grace-period":     "0s",
		"--http-address":          ":80",
		"--query.replica-label":   replicaLabel,
		"--store.sd-dns-interval": "5s",
		"--log.level":             "info",
		"--store.sd-interval":     "5s",
	})
	for _, addr := range storeAddresses {
		args = append(args, "--store="+addr)
	}

	if len(fileSDStoreAddresses) > 0 {
		queryFileSDDir := filepath.Join(sharedDir, "data", "querier", name)
		if err := os.MkdirAll(queryFileSDDir, 0777); err != nil {
			return nil, errors.Wrap(err, "create query dir failed")
		}

		if err := ioutil.WriteFile(queryFileSDDir+"/filesd.json", []byte(generateFileSD(fileSDStoreAddresses)), 0666); err != nil {
			return nil, errors.Wrap(err, "creating query SD config failed")
		}

		args = append(args, "--store.sd-files="+filepath.Join(queryFileSDDir, "filesd.json"))
	}

	return NewService(
		fmt.Sprintf("querier-%v", name),
		DefaultImage(),
		nil,
		e2e.NewCommand("query", args...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
		9091,
	), nil
}

func RemoteWriteEndpoint(addr string) string { return fmt.Sprintf("%s/api/v1/receive", addr) }

func NewReceiver(sharedDir string, name string, replicationFactor int, hashring ...receive.HashringConfig) (*Service, error) {
	dir := filepath.Join(sharedDir, "data", "receive", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "receive", name)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	b, err := json.Marshal(hashring)
	if err != nil {
		return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
	}

	if err := ioutil.WriteFile(filepath.Join(dir, "hashrings.json"), b, 0666); err != nil {
		return nil, errors.Wrap(err, "creating receive config")
	}

	return NewService(
		fmt.Sprintf("receive-%v", name),
		DefaultImage(),
		nil,
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":                              fmt.Sprintf("receive-%v", name),
			"--grpc-address":                            ":9091",
			"--grpc-grace-period":                       "0s",
			"--http-address":                            ":80",
			"--remote-write.address":                    ":81",
			"--label":                                   fmt.Sprintf(`receive="%s"`, name),
			"--tsdb.path":                               container,
			"--log.level":                               "info",
			"--receive.replication-factor":              strconv.Itoa(replicationFactor),
			"--receive.local-endpoint":                  ":9091",
			"--receive.hashrings-file":                  filepath.Join(container, "hashrings.json"),
			"--receive.hashrings-file-refresh-interval": "5s",
		})...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
		9091,
		81,
	), nil
}
