export OCI_ROOT_URL := "http://127.0.0.1:13030"
export OCI_NAMESPACE := "woof"
export OCI_DEBUG := "1"
#export OCI_CROSSMOUNT_NAMESPACE="myorg/other"
#export OCI_USERNAME="myuser"
#export OCI_PASSWORD="mypass"

conformance-push focus="" $OCI_TEST_PUSH="1":
  just conformance '{{focus}}'

conformance-pull focus="" $OCI_TEST_PULL="1":
  just conformance '{{focus}}'

conformance focus="":
  pushd distribution-spec/conformance \
    && go test -c \
    && ./conformance.test -test.failfast -ginkgo.fail-fast -ginkgo.focus="{{focus}}"

devenv-up:
  docker compose up -d
  sleep 1
  just init-cockroachdb
  just sqlx-migrate

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

export DATABASE_URL := "postgresql://root@localhost:26258/defaultdb?sslmode=disable"
sqlx-migrate:
  sqlx migrate --source db/postgres/migrations run

build:
  cargo build

run config $RUST_LOG="info,portfolio=debug,tower_http=debug,sqlx::query=off":
  ./target/debug/portfolio --config-file {{config}}

build-and-run config:
    just build
    just run {{config}}

we-build config:
  watchexec \
    -c \
    -w src \
    -w Cargo.toml \
    -w justfile \
    -e toml,rs \
    --restart \
    just build-and-run {{config}}
