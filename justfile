export OCI_ROOT_URL := "http://127.0.0.1:13030"
export OCI_NAMESPACE := "woof"
export OCI_DEBUG := "1"
export OCI_DELETE_MANIFEST_BEFORE_BLOBS := "1"

#export OCI_CROSSMOUNT_NAMESPACE="myorg/other"
#export OCI_USERNAME="myuser"
#export OCI_PASSWORD="mypass"

conformance-push focus="" $OCI_TEST_PUSH="1":
  just conformance '{{focus}}'

conformance-pull focus="" $OCI_TEST_PUSH="1":
  just conformance '{{focus}}'

conformance-management focus="" $OCI_TEST_CONTENT_MANAGEMENT="1":
  just conformance '{{focus}}'

conformance-discovery focus="" $OCI_TEST_CONTENT_DISCOVERY="1":
  just conformance '{{focus}}'

conformance-all focus="" $OCI_TEST_PUSH="1" $OCI_TEST_PULL="1" $OCI_TEST_CONTENT_DISCOVERY="1" $OCI_TEST_CONTENT_MANAGEMENT="1":
  just conformance '{{focus}}'

conformance focus="":
  pushd distribution-spec/conformance \
    && go test -c \
    && ./conformance.test -test.failfast -ginkgo.v -ginkgo.fail-fast

devenv-up:
  docker compose up -d
  sleep 1
  just init-cockroachdb
  just cockroachdb-migrate
  just postgresql-migrate

devenv-down:
  docker compose down
  docker volume ls -f name=portfolio -q \
    | xargs docker volume rm

devenv-cycle:
  just devenv-down
  just devenv-up

init-cockroachdb:
  docker exec -it portfolio-roach1-1 \
    ./cockroach --host=roach1:26257 init --insecure

cockroachdb-migrate $DATABASE_URL="postgresql://root@localhost:26258/defaultdb?sslmode=disable":
  sqlx migrate --source portfolio_postgres-s3/migrations run

postgresql-migrate $DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?sslmode=disable":
  sqlx migrate --source portfolio_postgres-s3/migrations run

build:
  cargo build

build-refactor:
  # requires cargo-limit to be installed
  reset
  (cargo lbuild --color=always 2>&1) | less -R

run config $RUST_LOG="info,portfolio=debug,tower_http=debug,sqlx::query=off":
  ./target/debug/portfolio --config-file {{config}}

refactor-and-run config:
    just build-refactor
    just run {{config}}

build-and-run config:
    just build
    just run {{config}}

watchexec target:
  watchexec \
    -c \
    -e toml,rs \
    -w justfile \
    -w Cargo.toml \
    -w objectstore/src \
    -w objectstore/Cargo.toml \
    -w portfolio_postgres-s3/src \
    -w portfolio_postgres-s3/Cargo.toml \
    -w portfolio/src \
    -w portfolio/Cargo.toml \
    -w portfoliod/src \
    -w portfoliod/Cargo.toml \
    --restart \
    just {{target}}

we-build-refactor:
  just watchexec build-refactor

we-build:
  just watchexec build

we-run-dev config:
  watchexec \
    -w target/debug/portfolio \
    --restart \
    just run {{config}}
