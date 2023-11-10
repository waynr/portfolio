# Architecture

## Layers

Portfolio is split into three primary layers:

* HTTP Server
* Core Abstractions
* Backends

These layers are combined to form a daemon binary in the `portfolio` crate.

### HTTP Server

**Crate:** [portfolio_http](../crates/portfolio_http)

The `portfolio-http` is where the distribution HTTP API spec is implemented.
This implementation is generic over traits defined in the `portfolio-core` repo
which provide methods that enable operations against backends implementing
those traits.

This HTTP API is known at the time of writing to pass the OCI Distribution
conformance tests.

### Core Abstractions

**Crate:** [portfolio_core](../crates/portfolio_core)

#### API Boundary Traits

Portfolio relies on a very intentional api contract between the
`portfolio-http` and all potential backends. Any backend that implements these
traits can be used to store/serve OCI Distribution content via a well-defined
and conformance test compliant HTTP API.

<dl>
<dt><code>RepositoryStoreManager</code></dt>
  <dd>
  Create & get <code>RepositoryStore</code> instances. Initalized from
  backend-specific config and passed to the HTTP service to be invoked on each
  HTTP request.
  </dd>
<dt><code>RepositoryStore</code></dt>
  <dd>
  Provides access to a <code>ManifestStore</code> and <code>BlobStore</code>
  instances for the sake of repository content management, handles session
  management (create, get, delete), and provides a tag listing method.
  </dd>
<dt><code>ManifestStore</code></dt>
  <dd>
  Implements manifest <code>head</code>, <code>get</code>, <code>put</code>,
  and <code>delete</code> operations for a Repository, provides a
  <code>get_referrers</code> method to implement the Referrers API.
  </dd>
<dt><code>BlobStore</code></dt>
  <dd>
  Implements blob <code>head</code>, <code>get</code>, <code>put</code>, and
  <code>delete</code> operations for a Repository, enables resuming a chunked
  upload session by providing access to a <code>BlobWriter</code>.
  </dd>
<dt><code>BlobWriter</code></dt>
  <dd>
  Provides chunked upload access for blobs.
  </dd>
<dt><code>Blob</code></dt>
  <dd>
  Provides accessor methods for blob metadata and allows each backend
  implementation to implement its own blob type.
  </dd>
<dt><code>Manifest</code></dt>
  <dd>
  Provides accessor methods for manifest metadata and allows each backend
  implementation to implement its own manifest type.
  </dd>
<dt><code>UploadSession</code></dt>
  <dd>
  Provides accessor methods for upload session metadata and allows each backend
  implementation to implement its own upload session type.
  </dd>
</dl>

### Backends

Backends may take any number of forms, but the current open source
implementation was designed in such a way as to clearly separate metadata and
bulk data storage.

#### Postgres + ObjectStore

**Crates:**
* [portfolio_backend_postgres](../crates/portfolio_backend_postgres)
* [portfolio_objectstore](../crates/portfolio_objectstore)

The only current backend implementation takes the approach of clearly
distinguishing between metadata and bulk data storage.

**Metadata** is information about the relationship between OCI Distribution
concepts - manifests, tags, blobs/layers, referrers as well as metadata about
each object managed by the registry such as content digest and bytes on disk.

**Bulk data storage** is exactly what it sounds like -- the storage of large
"blobs" of data. Blobs are keyed by UUID tied to their respective content
digest in the metadata table.

This implementation is split into two parts -- the actual backend in the
`portfolio-backend-postgres` crate that implements the API boundary traits in
terms of a Postgres data model and the `portfolio-objectstore` crate which
offers its own `ObjectStore` trait such that this Postgres backend can be (but
isn't at the time of writing) generic over alternative object store
implementations.
