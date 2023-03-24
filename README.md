# go-ds-blob

A cloudy blobstore-backed [datastore](https://github.com/ipfs/go-datastore)


This is a go-datastore adapter that uses several blob-store services offered by popular cloud providers. This
datastore uses the [Go Cloud Development Kit](https://github.com/google/go-cloud) ([homepage](https://gocloud.dev))
blob library as the storage backend. Multiple cloud providers are supported through this library.


## Why should you use this datastore?

 * I already run a go-datastore application and I want to make management easier
 * I am paying for empty block storage, e.g. badgerdb-on-ebs
 * I run workloads on multiple clouds
 * I have a large datastore that cannot fit on a single block-storage disk
 * I want to share a datastore with multiple processes, machines, containers, lambda/function
 * I want to use local files or cloud providers without plugins or rebuilding


## Why should I not use this datastore?

 * I want my data to be local (although, local file backend is also supported)
 * I frequently query data by attributes or relations
 * I need ACID transactions

## Get started

By default the backed is inferred from the name of the bucket you specify. If your default credentials are setup, you can
make use of the default credentials.

```
// bkt := "gs://my-bucket"
// bkt := "s3://my-bucket"
// bkt := "azblob://my-bucket"
// bkt := "file://my-bucket"

bkt := "mem://my-bucket"

d, _ := New(context.Background(), bucket)

```

## disclaimer

blob do not make a perfect datastore. Many blob-store services are eventually-consistent. Compared to other
databases, the query api is typically basic, and it's difficult to support all datastore features well.
However, while they may lack the rich query features often found in RDBMS or Document databases, they are
often a magnitude less expensive to operate, and performant at high scale.

## prior art

There is already an S3-only implementation maintained by Protocol Labs.
github.com/ipfs/go-ds-s3
