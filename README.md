# portfolio

Portfolio is a [distribution
implementation](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)
focused on safe concurrent object storage management via transactional metadata
and relatively simple abstractions between HTTP API and storage backends.

## Development Environment

### Requirements

* [rust](https://rustup.rs/)
* [docker compose](https://docs.docker.com/compose/install/)
* [Digitalocean account](https://www.digitalocean.com/)
  * You will need a (preferably) fresh Spaces bucket and access/secret key

### Setup

1. Bring up the docker compose dev env:
```
docker compose up -d
```
2. Take note of the postgres connection string in the output of `docker compose
   logs`. These should look something like the following (note that you will
   have to replace the network identifier with `localhost` later):
```
postgresql://root@71990c668326:26257/defaultdb?sslmode=disable
```
3. Retrieve your secret & access keys for Spaces.
4. Create a file called `dev-config.yml` that should look something like:
```
metadata:
  type: Postgres
  connection_string: postgresql://root@localhost:26257/defaultdb?sslmode=disable
objects:
  type: S3
  secret_key: <spaces-secret-key>
  access_key: <spaces-access-key>
  hostname: <bucket-region>.digitaloceanspaces.com
```

### Compile Time DB Query Validation

Because we use the `sqlx::query!` macro, which provides compile-time Postgres
query correctness guarantees, you will need to set a connection string variable
when compiling locally or if your text editor uses
[rust-analyzer](https://rust-analyzer.github.io/) (which it should!), otherwise
you will see DB connection errors at compile time. For example, if running
`cargo build` to verify your changes build then you would need the following in
your shell:

```
export DATABASE_URL=postgresql://root@localhost:26257/defaultdb?sslmode=disable
```
