# Development Environment

## Requirements

* [rust](https://rustup.rs/)
* [just](https://just.systems/man/en/)
* [docker compose](https://docs.docker.com/compose/install/)
* An S3 API and credentials for bucket access.

## Setup

1. Bring up the docker compose dev env:
```shell
just devenv-cycle
```
2. Take note of the postgres connection string in the output of `docker compose
   logs`. These should look something like the following (note that you will
   have to replace the network identifier with `localhost` later):
```
postgresql://postgres:password@localhost:26258/defaultdb?sslmode=disable
```
3. Retrieve your S3 API secret & access keys.
4. Create a file called `dev-config.yml` that should look something like:
```yaml
backend:
  type: PostgresS3
  postgres:
    connection_string: postgresql://root@localhost:26257/defaultdb?sslmode=disable
  s3:
    secret_key: <bucket-secret-key>
    access_key: <bucket-access-key>
    hostname: <bucket-region>.<s3-api-provider-hostname>
    bucket_name: <bucket-name>

```
5. Start local server
```
just we-run-dev dev-config.yml
```
