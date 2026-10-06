package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3 talks to any S3-compatible store (Cloudflare R2, MinIO, AWS S3).
type S3 struct {
	c      *s3.Client
	bucket string
}

// NewS3 parses s3://bucket?endpoint=URL&region=R&path_style=true.
func NewS3(ctx context.Context, raw string) (*S3, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	region := q.Get("region")
	if region == "" {
		region = "auto"
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		// The cores own retries: a hidden SDK retry of a put-if-absent could
		// turn our own landed write into a 412 the caller cannot attribute.
		config.WithRetryMaxAttempts(1),
	)
	if err != nil {
		return nil, err
	}
	c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if ep := q.Get("endpoint"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
		o.UsePathStyle = q.Get("path_style") == "true"
		// Default CRC checksums are not supported by every S3-compatible
		// store; send them only when an operation requires them.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	if u.Host == "" {
		return nil, errors.New("objstore: s3 URL needs a bucket")
	}
	return &S3{c: c, bucket: u.Host}, nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		switch re.HTTPStatusCode() {
		case http.StatusNotFound:
			return ErrNotFound
		case http.StatusPreconditionFailed:
			return ErrPrecondition
		}
		// 409 ConditionalRequestConflict (concurrent conditional writes) and
		// everything else is transient: retry the same request.
	}
	return err
}

func (s *S3) Get(ctx context.Context, key string, off, n int64) ([]byte, error) {
	in := &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}
	if n > 0 {
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1))
	} else if off > 0 {
		in.Range = aws.String(fmt.Sprintf("bytes=%d-", off))
	}
	out, err := s.c.GetObject(ctx, in)
	if err != nil {
		return nil, mapErr(err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *S3) Put(ctx context.Context, key string, data []byte, ifAbsent bool) error {
	in := &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}
	if ifAbsent {
		in.IfNoneMatch = aws.String("*")
	}
	_, err := s.c.PutObject(ctx, in)
	return mapErr(err)
}

func (s *S3) Head(ctx context.Context, key string) (int64, error) {
	out, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return 0, mapErr(err)
	}
	return aws.ToInt64(out.ContentLength), nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	_, err := s.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	if err = mapErr(err); errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (s *S3) List(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, error) {
	var out []ObjectInfo
	in := &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix}
	if after != "" {
		in.StartAfter = &after
	}
	for {
		if limit > 0 {
			in.MaxKeys = aws.Int32(int32(min(limit-len(out), 1000)))
		}
		page, err := s.c.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, mapErr(err)
		}
		for _, o := range page.Contents {
			out = append(out, ObjectInfo{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), LastModified: aws.ToTime(o.LastModified)})
		}
		if !aws.ToBool(page.IsTruncated) || (limit > 0 && len(out) >= limit) {
			return out, nil
		}
		in.ContinuationToken = page.NextContinuationToken
		in.StartAfter = nil
	}
}
