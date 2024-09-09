package dsblob

import (
	"context"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"

	dstest "github.com/ipfs/go-datastore/test"
)

func TestSuiteCloudDatastore(t *testing.T) {
	//
	bucketName := "mem://test-bucket"
	bucket, err := blob.OpenBucket(context.Background(), bucketName)
	if err != nil {
		t.Fatal(err)
	}

	cds := NewWithBucket(bucket, "memory")

	t.Run("basic operations", func(t *testing.T) {
		dstest.SubtestBasicPutGet(t, cds)
	})
	t.Run("not found operations", func(t *testing.T) {
		dstest.SubtestNotFounds(t, cds)
	})
	t.Run("many puts and gets, query", func(t *testing.T) {
		dstest.SubtestManyKeysAndQuery(t, cds)
	})
	t.Run("return sizes", func(t *testing.T) {
		dstest.SubtestReturnSizes(t, cds)
	})
}
