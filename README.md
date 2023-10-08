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
2. Initialized the cockroachdb cluster:
```
docker exec -it portfolio-region1-1 ./cockroach init --insecure
```
3. Take note of the postgres connection string in the output of `docker compose
   logs`. These should look something like the following (note that you will
   have to replace the network identifier with `localhost` later):
```
postgresql://root@71990c668326:26257/defaultdb?sslmode=disable
```
4. Retrieve your secret & access keys for Spaces.
5. Create a file called `dev-config.yml` that should look something like:
```
metadata:
  type: Postgres
  connection_string: postgresql://root@localhost:26257/defaultdb?sslmode=disable
objects:
  type: S3
  secret_key: <spaces-secret-key>
  access_key: <spaces-access-key>
  hostname: <bucket-region>.digitaloceanspaces.com
  bucket_name: <bucket-name>

```
6. Run DB migrations:
```
export DATABASE_URL=postgresql://root@localhost:26257/defaultdb?sslmode=disable
cargo install sqlx-cli
sqlx migrate run --source db/postgres/migrations
```

7. Start local server
```
cargo run
```
