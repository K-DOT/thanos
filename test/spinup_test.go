// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package test_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"google.golang.org/grpc"
)

func storeGateway(http, grpc address, bucketConfig []byte, relabelConfig []byte) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		GRPC: grpc,
		schedule: func(workDir string) (Exec, error) {
			dbDir := filepath.Join(workDir, "data", "store-gateway", http.Port)

			if err := os.MkdirAll(dbDir, 0777); err != nil {
				return nil, errors.Wrap(err, "creating store gateway dir failed")
			}

			return newCmdExec(exec.Command("thanos",
				"store",
				"--debug.name", fmt.Sprintf("store-gw-%s", http.Port),
				"--data-dir", dbDir,
				"--grpc-address", grpc.HostPort(),
				"--grpc-grace-period", "0s",
				"--http-address", http.HostPort(),
				"--log.level", "debug",
				"--objstore.config", string(bucketConfig),
				// Accelerated sync time for quicker test (3m by default).
				"--sync-block-duration", "5s",
				"--selector.relabel-config", string(relabelConfig),
				"--experimental.enable-index-header",
			)), nil
		},
	}
}

func alertManager(http address) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		schedule: func(workDir string) (Exec, error) {
			dir := filepath.Join(workDir, "data", "alertmanager", http.Port)

			if err := os.MkdirAll(dir, 0777); err != nil {
				return nil, errors.Wrap(err, "creating alertmanager dir failed")
			}
			config := `
route:
  group_by: ['alertname']
  group_wait: 1s
  group_interval: 1s
  receiver: 'null'
receivers:
- name: 'null'
`
			if err := ioutil.WriteFile(dir+"/config.yaml", []byte(config), 0666); err != nil {
				return nil, errors.Wrap(err, "creating alertmanager config file failed")
			}
			return newCmdExec(exec.Command(e2eutil.AlertmanagerBinary(),
				"--config.file", dir+"/config.yaml",
				"--web.listen-address", http.HostPort(),
				"--cluster.listen-address", "",
				"--log.level", "debug",
			)), nil
		},
	}
}

func rule(http, grpc address, ruleDir string, amCfg []byte, queryCfg []byte) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		GRPC: grpc,
		schedule: func(workDir string) (Exec, error) {
			err := ioutil.WriteFile(filepath.Join(workDir, "query.cfg"), queryCfg, 0666)
			if err != nil {
				return nil, errors.Wrap(err, "creating query config for ruler")
			}
			args := []string{
				"rule",
				"--debug.name", fmt.Sprintf("rule-%s", http.Port),
				"--label", fmt.Sprintf(`replica="%s"`, http.Port),
				"--data-dir", filepath.Join(workDir, "data"),
				"--rule-file", filepath.Join(ruleDir, "*.yaml"),
				"--eval-interval", "1s",
				"--alertmanagers.config", string(amCfg),
				"--alertmanagers.sd-dns-interval", "5s",
				"--grpc-address", grpc.HostPort(),
				"--grpc-grace-period", "0s",
				"--http-address", http.HostPort(),
				"--log.level", "debug",
				"--query.config-file", filepath.Join(workDir, "query.cfg"),
				"--query.sd-dns-interval", "5s",
				"--resend-delay", "5s",
			}
			return newCmdExec(exec.Command("thanos", args...)), nil
		},
	}
}

type sameProcessGRPCServiceExec struct {
	addr   string
	stdout io.Writer
	stderr io.Writer

	ctx     context.Context
	cancel  context.CancelFunc
	srvChan <-chan error
	srv     *grpc.Server
}

func (c *sameProcessGRPCServiceExec) Start(stdout io.Writer, stderr io.Writer) error {
	c.stderr = stderr
	c.stdout = stdout

	if c.ctx != nil {
		return errors.New("process already started")
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	l, err := net.Listen("tcp", c.addr)
	if err != nil {
		return errors.Wrap(err, "listen API address")
	}

	srvChan := make(chan error)
	go func() {
		defer close(srvChan)
		if err := c.srv.Serve(l); err != nil {
			srvChan <- err
			_, _ = c.stderr.Write([]byte(fmt.Sprintf("server failed: %s", err)))
		}
	}()
	c.srvChan = srvChan
	return nil
}

func (c *sameProcessGRPCServiceExec) Wait() error {
	err := <-c.srvChan
	if c.ctx.Err() == nil && err != nil {
		return err
	}
	return err
}

func (c *sameProcessGRPCServiceExec) Kill() error {
	c.cancel()
	c.srv.Stop()

	return nil
}

func (c *sameProcessGRPCServiceExec) String() string {
	return fmt.Sprintf("gRPC service %v", c.addr)
}

func fakeStoreAPI(grpcAddr address, svc storepb.StoreServer) *serverScheduler {
	return &serverScheduler{
		GRPC: grpcAddr,
		schedule: func(_ string) (Exec, error) {

			srv := grpc.NewServer()
			storepb.RegisterStoreServer(srv, svc)

			return &sameProcessGRPCServiceExec{addr: grpcAddr.HostPort(), srv: srv}, nil
		},
	}
}

func minio(http address, config s3.Config) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		schedule: func(workDir string) (Exec, error) {
			dbDir := filepath.Join(workDir, "data", "minio", http.Port)
			if err := os.MkdirAll(dbDir, 0777); err != nil {
				return nil, errors.Wrap(err, "creating minio dir failed")
			}

			cmd := exec.Command(e2eutil.MinioBinary(),
				"server",
				"--address", http.HostPort(),
				dbDir,
			)
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("MINIO_ACCESS_KEY=%s", config.AccessKey),
				fmt.Sprintf("MINIO_SECRET_KEY=%s", config.SecretKey))

			return newCmdExec(cmd), nil
		},
	}
}

// NOTE: It is important to install Thanos before using this function to compile latest changes.
// This means that export GOCACHE=/unique/path is must have to avoid having this test cached locally.
func e2eSpinup(t testing.TB, ctx context.Context, cmds ...scheduler) (exit chan struct{}, err error) {
	return e2eSpinupWithS3ObjStorage(t, ctx, address{}, nil, cmds...)
}

func e2eSpinupWithS3ObjStorage(t testing.TB, ctx context.Context, minioAddr address, s3Config *s3.Config, cmds ...scheduler) (exit chan struct{}, err error) {
	dir, err := ioutil.TempDir("", "spinup_test")
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			if rerr := os.RemoveAll(dir); rerr != nil {
				t.Log(rerr)
			}
		}
	}()

	var s3Exit chan struct{}
	if s3Config != nil {
		s3Exit, err = e2eSpinupWithS3ObjStorage(t, ctx, address{}, nil, minio(minioAddr, *s3Config))
		if err != nil {
			return nil, errors.Wrap(err, "start minio")
		}

		ctx, cancel := context.WithCancel(ctx)
		if err := runutil.Retry(time.Second, ctx.Done(), func() error {
			select {
			case <-s3Exit:
				cancel()
				return nil
			default:
			}

			bkt, _, err := s3.NewTestBucketFromConfig(t, "eu-west1", *s3Config, false)
			if err != nil {
				return errors.Wrap(err, "create bkt client for minio healthcheck")
			}

			return bkt.Close()
		}); err != nil {
			return nil, errors.Wrap(err, "minio not ready in time")
		}
	}

	var g run.Group

	// Interrupt go routine.
	{
		ctx, cancel := context.WithCancel(ctx)
		g.Add(func() error {
			// This go routine will return only when:
			// 1) Any other process from group exited unexpectedly
			// 2) Global context will be cancelled.
			// 3) Minio (if started) exited unexpectedly.

			if s3Exit != nil {
				select {
				case <-ctx.Done():
				case <-s3Exit:
				}
				return nil
			}

			<-ctx.Done()
			return nil

		}, func(error) {
			cancel()
			if err := os.RemoveAll(dir); err != nil {
				t.Log(err)
			}
		})
	}

	var stdFiles []*os.File
	// Run go routine for each command.
	for _, command := range cmds {
		c, err := command.Schedule(dir)
		if err != nil {
			return nil, err
		}
		// Store buffers in temp files to avoid excessive memory consumption.
		stdout, err := ioutil.TempFile(dir, "stdout")
		if err != nil {
			return nil, errors.Wrap(err, "create file for stdout")
		}

		stderr, err := ioutil.TempFile(dir, "stderr")
		if err != nil {
			return nil, errors.Wrap(err, "create file for stderr")
		}

		stdFiles = append(stdFiles, stdout, stderr)
		if err := c.Start(stdout, stderr); err != nil {
			// Let already started commands finish.
			go func() { _ = g.Run() }()
			return nil, errors.Wrap(err, "start")
		}

		cmd := c
		g.Add(func() error {
			return errors.Wrap(cmd.Wait(), cmd.String())
		}, func(error) {
			// This's accepted scenario to kill a process immediately for sure and run tests as fast as possible.
			_ = cmd.Kill()
		})
	}

	exit = make(chan struct{})
	go func(g run.Group) {
		if err := g.Run(); err != nil && ctx.Err() == nil {
			t.Errorf("Some process exited unexpectedly: %v", err)
		}

		if s3Exit != nil {
			<-s3Exit
		}

		printAndCloseFiles(t, stdFiles)
		close(exit)
	}(g)

	return exit, nil
}

func generateFileSD(addresses []address) string {
	conf := "[ { \"targets\": ["
	for index, addr := range addresses {
		conf += fmt.Sprintf("\"%s\"", addr.HostPort())
		if index+1 < len(addresses) {
			conf += ","
		}
	}
	conf += "] } ]"
	return conf
}

func printAndCloseFiles(t testing.TB, files []*os.File) {
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	for _, f := range files {
		info, err := f.Stat()
		if err != nil {
			t.Error(err)
		}

		if info.Size() == 0 {
			continue
		}

		if _, err := f.Seek(0, 0); err != nil {
			t.Error(err)
		}
		t.Logf("-------------------------------------------")
		t.Logf("-------------------  %s  ------------------", f.Name())
		t.Logf("-------------------------------------------")

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			t.Log(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			t.Error(err)
		}
	}
}
