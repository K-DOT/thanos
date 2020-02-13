// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// NOTE: by using aggregation all results are now unsorted.
const queryUpWithoutInstance = "sum(up) without (instance)"

// defaultPromConfig returns Prometheus config that sets Prometheus to:
// * expose 2 external labels, source and replica.
// * scrape fake target. This will produce up == 0 metric which we can assert on.
// * optionally remote write endpoint to write into.
func defaultPromConfig(name string, replica int, remoteWriteEndpoint string) string {
	config := fmt.Sprintf(`
global:
  external_labels:
    prometheus: %v
    replica: %v
scrape_configs:
- job_name: 'test'
  static_configs:
  - targets: ['fake']
`, name, replica)

	if remoteWriteEndpoint != "" {
		config = fmt.Sprintf(`
%s
remote_write:
- url: "%s"
`, config, remoteWriteEndpoint)
	}
	return config
}

func sortResults(res model.Vector) {
	sort.Slice(res, func(i, j int) bool {
		return res[i].String() < res[j].String()
	})
}

func TestQuery(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_query")
	testutil.Ok(t, err)
	defer s.Close()

	receiver, err := e2ethanos.NewReceiver(s.SharedDir(), "1", 1)
	testutil.Ok(t, err)

	testutil.Ok(t, s.StartAndWaitReady(receiver))

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "alone", defaultPromConfig("prom-alone", 0, ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "remote-and-sidecar", defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, e2ethanos.RemoteWriteEndpoint(receiver.HTTPEndpoint())), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom3, sidecar3, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "ha1", defaultPromConfig("prom-ha", 0, ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom4, sidecar4, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "ha2", defaultPromConfig("prom-ha", 1, ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)

	testutil.Ok(t, s.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2, prom3, sidecar3, prom4, sidecar4))

	// Querier. Both fileSD and directly by flags.
	q, err := e2ethanos.NewQuerier(
		s.SharedDir(), "1",
		[]string{sidecar1.GRPCEndpoint(), sidecar2.GRPCEndpoint(), receiver.GRPCEndpoint()},
		[]string{sidecar3.GRPCEndpoint(), sidecar4.GRPCEndpoint()},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// TODO(bwplotka): Scenario should give us info if one container exited unexpectedly.
	// Without deduplication.
	queryAndAssert(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
		Deduplicate: true,
	}, []model.Metric{
		{
			"job":        "test",
			"prometheus": "prom-alone",
			"replica":    "0",
		},
		{
			"job":        "test",
			"prometheus": "prom-both-remote-write-and-sidecar",
			"receive":    model.LabelValue(receiver.Endpoint(81)),
			"replica":    model.LabelValue("1234"),
		},
		{
			"job":        "test",
			"prometheus": "prom-both-remote-write-and-sidecar",
			"replica":    model.LabelValue("1234"),
		}, {
			"job":        "test",
			"prometheus": "prom-ha",
			"replica":    model.LabelValue("0"),
		}, {
			"job":        "test",
			"prometheus": "prom-ha",
			"replica":    model.LabelValue("1"),
		},
	})

	// With deduplication.
	queryAndAssert(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "test",
			"prometheus": "prom-alone",
		},
		{
			"job":        "test",
			"prometheus": "prom-both-remote-write-and-sidecar",
		},
		{
			"job":        "test",
			"prometheus": "prom-ha",
		},
	})
}

func urlParse(t *testing.T, addr string) *url.URL {
	u, err := url.Parse(addr)
	testutil.Ok(t, err)

	return u
}

func queryAndAssert(t *testing.T, ctx context.Context, addr string, query string, opts promclient.QueryOptions, expected []model.Metric) {
	var result model.Vector
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, addr), query, time.Now(), opts)
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		expectedRes := 4
		if len(res) != expectedRes {
			return errors.Errorf("unexpected result size, expected %d; result: %v", expectedRes, res)
		}
		result = res
		return nil
	}))

	sortResults(result)
	for i, exp := range expected {
		testutil.Equals(t, exp, result[i].Metric)
	}
}
