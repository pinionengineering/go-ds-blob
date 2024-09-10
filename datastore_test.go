package dsblob

import (
	"context"
	"fmt"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"

	dstest "github.com/ipfs/go-datastore/test"
)

func TestSuiteCloudDatastore(t *testing.T) {
	ctx := context.Background()

	for i, subtest := range dstest.BasicSubtests {
		// bkt, _ := blob.OpenBucket(ctx, "gs://dsblobtest")
		bkt, _ := blob.OpenBucket(ctx, "mem://")
		name := fmt.Sprintf("test_%d", i)
		ds := NewWithBucket(bkt, name)
		t.Run(name, func(ti *testing.T) {
			subtest(ti, ds)
		})
	}
}
