package dsblob

// Documentation:
// https://gocloud.dev/howto/blob/#s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob/s3blob"
)

// NewS3WithConfig creates a new CloudDatastore with the given bucket name and aws.Config.
func NewS3WithConfig(ctx context.Context, bucketName string, cfg aws.Config) (*CloudDatastore, error) {
	s3Client := s3v2.NewFromConfig(cfg)
	b, err := s3blob.OpenBucketV2(ctx, s3Client, bucketName, nil)
	if err != nil {
		return nil, err
	}
	return NewWithBucket(b), nil
}
