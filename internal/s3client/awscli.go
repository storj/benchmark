// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package s3client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/zeebo/errs"
)

// AWSCLIError is class for minio errors.
var AWSCLIError = errs.Class("aws-cli error")

// AWSCLI implements basic S3 Client with aws-cli.
type AWSCLI struct {
	conf Config
}

// NewAWSCLI creates new Client.
func NewAWSCLI(conf Config) (Client, error) {
	if !strings.HasPrefix(conf.S3Gateway, "https://") &&
		!strings.HasPrefix(conf.S3Gateway, "http://") {
		conf.S3Gateway = "http://" + conf.S3Gateway
	}
	return &AWSCLI{conf}, nil
}

func (client *AWSCLI) cmd(subargs ...string) *exec.Cmd {
	args := []string{
		"--endpoint", client.conf.S3Gateway,
	}

	if client.conf.NoSSL {
		args = append(args, "--no-verify-ssl")
	}
	args = append(args, subargs...)

	/* #nosec G204 */ // Go exec.Command doesn't allow to execute more than one
	// command it passes the arguments to the command properly escaped which are
	// only interpreted by the OS as the arguments of the indicated program (.i.e
	// aws).
	cmd := exec.Command("aws", args...)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+client.conf.AccessKey,
		"AWS_SECRET_ACCESS_KEY="+client.conf.SecretKey,
	)
	return cmd
}

// MakeBucket makes a new bucket.
func (client *AWSCLI) MakeBucket(bucket, location string) error {
	cmd := client.cmd("s3", "mb", "s3://"+bucket, "--region", location)
	out, err := cmd.Output()
	if err != nil {
		return AWSCLIError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// RemoveBucket removes a bucket.
func (client *AWSCLI) RemoveBucket(bucket string) error {
	cmd := client.cmd("s3", "rb", "s3://"+bucket)
	out, err := cmd.Output()
	if err != nil {
		return AWSCLIError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// ListBuckets lists all buckets.
func (client *AWSCLI) ListBuckets() ([]string, error) {
	cmd := client.cmd("s3api", "list-buckets", "--output", "json")
	jsondata, err := cmd.Output()
	if err != nil {
		return nil, AWSCLIError.Wrap(fullExitError(err, string(jsondata)))
	}

	var response struct {
		Buckets []struct {
			Name string `json:"Name"`
		} `json:"Buckets"`
	}

	err = json.Unmarshal(jsondata, &response)
	if err != nil {
		return nil, AWSCLIError.Wrap(fullExitError(err, ""))
	}

	names := []string{}
	for _, bucket := range response.Buckets {
		names = append(names, bucket.Name)
	}

	return names, nil
}

// Upload uploads object data to the specified path.
func (client *AWSCLI) Upload(bucket, objectName string, data []byte) error {
	// TODO: add upload threshold
	cmd := client.cmd("s3", "cp", "-", "s3://"+bucket+"/"+objectName)
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.Output()
	if err != nil {
		return AWSCLIError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// UploadMultipart uses multipart uploads, has hardcoded threshold.
func (client *AWSCLI) UploadMultipart(bucket, objectName string, data []byte, threshold int) error {
	// TODO: add upload threshold
	cmd := client.cmd("s3", "cp", "-", "s3://"+bucket+"/"+objectName)
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.Output()
	if err != nil {
		return AWSCLIError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// Download downloads object data.
func (client *AWSCLI) Download(bucket, objectName string, buffer []byte) ([]byte, error) {
	cmd := client.cmd("s3", "cp", "s3://"+bucket+"/"+objectName, "-")

	buf := &bufferWriter{buffer[:0]}
	cmd.Stdout = buf
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return nil, AWSCLIError.Wrap(fullExitError(err, string(buf.data)))
	}

	return buf.data, nil
}

type bufferWriter struct {
	data []byte
}

func (b *bufferWriter) Write(data []byte) (n int, err error) {
	b.data = append(b.data, data...)
	return len(data), nil
}

// Delete deletes object.
func (client *AWSCLI) Delete(bucket, objectName string) error {
	cmd := client.cmd("s3", "rm", "s3://"+bucket+"/"+objectName)
	out, err := cmd.Output()
	if err != nil {
		return AWSCLIError.Wrap(fullExitError(err, string(out)))
	}
	return nil
}

// ListObjects lists objects.
func (client *AWSCLI) ListObjects(bucket, prefix string) ([]string, error) {
	cmd := client.cmd("s3api", "list-objects",
		"--output", "json",
		"--bucket", bucket,
		"--prefix", prefix,
		"--delimiter", "/")

	jsondata, err := cmd.Output()
	if err != nil {
		return nil, AWSCLIError.Wrap(fullExitError(err, string(jsondata)))
	}

	var response struct {
		Contents []struct {
			Key string `json:"Key"`
		} `json:"Contents"`
		CommonPrefixes []struct {
			Key string `json:"Prefix"`
		} `json:"CommonPrefixes"`
	}

	err = json.Unmarshal(jsondata, &response)
	if err != nil {
		return nil, AWSCLIError.Wrap(fullExitError(err, ""))
	}

	names := []string{}
	for _, object := range response.Contents {
		names = append(names, object.Key)
	}
	for _, object := range response.CommonPrefixes {
		names = append(names, object.Key)
	}

	return names, nil
}

// fullExitError returns error string with the Stderr output.
func fullExitError(err error, msg string) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return fmt.Errorf("%w\n%v\n%s", exitErr, string(exitErr.Stderr), msg)
	}
	return err
}
