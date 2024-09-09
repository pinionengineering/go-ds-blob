package dsblob

// Documentation:
// https://gocloud.dev/howto/blob/#gcs

import (
	"context"

	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
)

// NewGCPWithCredentials creates a new CloudDatastore with a GCP bucket.
// The user must separately authenticate with GCP and provide the credentials.
// bucketName should look like "my-bucket"
func NewGCPWithCredentials(ctx context.Context, creds *google.Credentials, bucketName string) (*CloudDatastore, error) {
	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, err
	}
	b, err := gcsblob.OpenBucket(ctx, client, bucketName, nil)
	if err != nil {
		return nil, err
	}
	return NewWithBucket(b, bucketName), nil
}
