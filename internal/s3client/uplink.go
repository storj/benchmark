// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package s3client

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/zeebo/errs"
)

// UplinkError is class for minio errors
var UplinkError = errs.Class("uplink error")

// Uplink implements basic S3 Client with uplink
type Uplink struct {
	conf Config
}

// NewUplink creates new Client
func NewUplink(conf Config) (Client, error) {
	client := &Uplink{conf}

	if client.conf.ConfigDir != "" {
		fmt.Printf("Using existing uplink config at %s\n", client.conf.ConfigDir)
		return client, nil
	}

	if conf.Access == "" {
		return nil, UplinkError.New("%s", "access cannot be empty")
	}

	return client, nil
}

func (client *Uplink) cmd(subargs ...string) *exec.Cmd {
	args := make([]string, 0, len(subargs)+2)
	args = append(args, subargs...)

	if client.conf.ConfigDir != "" {
		args = append(args, "--config-dir", client.conf.ConfigDir)
	} else {
		args = append(args, "--access", client.conf.Access)
	}

	cmd := exec.Command("uplink", args...)
	return cmd
}

// MakeBucket makes a new bucket
func (client *Uplink) MakeBucket(bucket, location string) error {
	cmd := client.cmd("mb", "s3://"+bucket)
	out, err := cmd.Output()
	if err != nil {
		return UplinkError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// RemoveBucket removes a bucket
func (client *Uplink) RemoveBucket(bucket string) error {
	cmd := client.cmd("rb", "s3://"+bucket)
	out, err := cmd.Output()
	if err != nil {
		return UplinkError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// ListBuckets lists all buckets
func (client *Uplink) ListBuckets() ([]string, error) {
	cmd := client.cmd("ls")
	data, err := cmd.Output()
	if err != nil {
		return nil, UplinkError.Wrap(fullExitError(err, string(data)))
	}

	names := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	return names, nil
}

// Upload uploads object data to the specified path
func (client *Uplink) Upload(bucket, objectName string, data []byte) error {
	// TODO: add upload threshold
	cmd := client.cmd("put", "s3://"+bucket+"/"+objectName)
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.Output()
	if err != nil {
		return UplinkError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// Download downloads object data
func (client *Uplink) Download(bucket, objectName string, buffer []byte) ([]byte, error) {
	cmd := client.cmd("cat", "s3://"+bucket+"/"+objectName)
	out, err := cmd.Output()
	if err != nil {
		return nil, UplinkError.Wrap(fullExitError(err, string(out)))
	}
	return out, nil
}

// Delete deletes object
func (client *Uplink) Delete(bucket, objectName string) error {
	cmd := client.cmd("rm", "s3://"+bucket+"/"+objectName)
	out, err := cmd.Output()
	if err != nil {
		return UplinkError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// ListObjects lists objects
func (client *Uplink) ListObjects(bucket, prefix string) ([]string, error) {
	cmd := client.cmd("ls", "s3://"+bucket+"/"+prefix)
	data, err := cmd.Output()
	if err != nil {
		return nil, UplinkError.Wrap(fullExitError(err, string(data)))
	}

	names := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	return names, nil
}
