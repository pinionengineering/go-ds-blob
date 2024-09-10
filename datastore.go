package dsblob

import (
	"context"
	"io"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

var (
	_ datastore.Datastore = (*CloudDatastore)(nil)
)

// implements ipfs/go-datastore CloudDatastore interface
// uses google/go-cloud blob.Bucket to store data
type CloudDatastore struct {
	bucket     *blob.Bucket
	bucketName string
}

// Create a new CloudDatastore using a bucket name and default parameters
// appropriate for the bucket type.
// GCS, S3, Azure, memory, and file buckets are suported.
// each system has its own default parameters.
// see https://gocloud.dev/howto/blob/ for information on each system.
//
// bucketName should have an approppriate prefix for the bucket type.
// e.g.
//   - "gs://my-bucket" for GCS
//   - "s3://my-bucket" for S3
//   - "azblob://my-bucket" for Azure
//   - "file://my-bucket" for file
//   - "mem://my-bucket" for memory
func New(ctx context.Context, bucketName string) (*CloudDatastore, error) {
	bucket, err := blob.OpenBucket(context.Background(), bucketName)
	if err != nil {
		return nil, err
	}
	return NewWithBucket(bucket, bucketName), nil
}

// NewDatastore returns a new Datastore based on a google/go-cloud blob.Bucket
func NewWithBucket(bucket *blob.Bucket, bucketName string) *CloudDatastore {
	return &CloudDatastore{
		bucket:     bucket,
		bucketName: bucketName,
	}
}

// Has returns whether the `key` is mapped to a `value`.
func (cds *CloudDatastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	return cds.bucket.Exists(ctx, key.String())
}

// GetSize returns the size of the `value` named by `key`.
// In some contexts, it may be much cheaper to only get the size of the
// value rather than retrieving the value itself.
func (cds *CloudDatastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	attrs, err := cds.bucket.Attributes(ctx, key.String())
	if gcerrors.Code(err) == gcerrors.NotFound {
		return -1, datastore.ErrNotFound
	}
	if err != nil {
		return -1, err
	}
	return int(attrs.Size), nil
}

// Close closes the Datastore
func (cds *CloudDatastore) Close() error {
	return cds.bucket.Close()
}

// Delete removes a key from the Datastore
func (cds *CloudDatastore) Delete(ctx context.Context, key datastore.Key) error {
	err := cds.bucket.Delete(ctx, key.String())
	if gcerrors.Code(err) == gcerrors.NotFound {
		return nil
	}
	return err
}

// Get retrieves a value from the Datastore
func (cds *CloudDatastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	r, err := cds.bucket.NewReader(ctx, key.String(), nil)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return nil, datastore.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// Put stores a value in the Datastore
func (cds *CloudDatastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	w, err := cds.bucket.NewWriter(ctx, key.String(), nil)
	if err != nil {
		return err
	}
	if _, err := w.Write(value); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// Sync synchronizes the Datastore
// Writes from the put method are persisted when the writer is closed.
// however, underlying implementaions may have eventual consistency.
func (cds *CloudDatastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

// Query searches the Datastore
// Unfortunately, orders and filters are done locally in ths implementation
func (cds *CloudDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	cleaned := datastore.NewKey(q.Prefix)

	bktlist := cds.bucket.List(&blob.ListOptions{
		Prefix:    cleaned.String(),
		Delimiter: "",
	})
	bktCtx, bktCancel := context.WithCancel(ctx)
	iter := query.Iterator{
		Next: func() (query.Result, bool) {
			qres := query.Result{}
			bktres, err := bktlist.Next(bktCtx)
			if err == io.EOF {
				return qres, false
			}
			if err != nil {
				qres.Error = err
				return qres, false
			}
			qres.Key = bktres.Key
			qres.Size = int(bktres.Size)
			if !q.KeysOnly {
				rdr, err := cds.bucket.NewReader(bktCtx, bktres.Key, nil)
				if err != nil {
					qres.Error = err
					return qres, false
				}
				defer rdr.Close()
				val, err := io.ReadAll(rdr)
				if err != nil {
					qres.Error = err
					return qres, false
				}
				qres.Value = val
			}
			return qres, true
		},
		Close: func() error {
			bktCancel()
			return nil
		},
	}

	return query.NaiveQueryApply(q, query.ResultsFromIterator(q, iter)), nil
}
