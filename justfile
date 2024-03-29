export OCI_ROOT_URL := "http://127.0.0.1:13030"
export OCI_NAMESPACE := "woof"
export OCI_DEBUG := "1"
export OCI_DELETE_MANIFEST_BEFORE_BLOBS := "1"

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
  just postgresql-migrate

devenv-down:
  docker compose down
  docker volume ls -f name=portfolio -q \
    | xargs docker volume rm

devenv-cycle:
  just devenv-down
  just devenv-up

postgresql-migrate $DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?sslmode=disable":
  sqlx migrate --source crates/portfolio_backend_postgres/migrations run

build:
  cargo build

doc targetdir:
  reset
  cargo doc
  rsync -av ./target/doc/ {{targetdir}}

build-refactor:
  # requires cargo-limit to be installed
  reset
  (cargo lbuild --color=always 2>&1) | less -R

doc-refactor targetdir:
  # requires cargo-limit to be installed
  reset
  (cargo ldoc --color=always 2>&1) | less -R
  rsync -av ./target/doc/ {{targetdir}}

run config $RUST_LOG="info,portfolio=debug,tower_http=debug,sqlx::query=off":
  ./target/debug/portfolio --config-file {{config}}

refactor-and-run config:
    just build-refactor
    just run {{config}}

build-and-run config:
    just build
    just run {{config}}

watchexec +args:
  watchexec \
    -c \
    -e toml,rs \
    -w justfile \
    -w Cargo.toml \
    -w crates/portfolio_objectstore/src \
    -w crates/portfolio_objectstore/Cargo.toml \
    -w crates/portfolio_backend_postgres/src \
    -w crates/portfolio_backend_postgres/Cargo.toml \
    -w crates/portfolio_http/src \
    -w crates/portfolio_http/Cargo.toml \
    -w crates/portfolio_core/src \
    -w crates/portfolio_core/Cargo.toml \
    -w crates/portfolio/src \
    -w crates/portfolio/Cargo.toml \
    --restart \
    just {{args}}

we-build-refactor:
  just watchexec build-refactor

we-doc-refactor targetdir:
  just watchexec doc-refactor {{targetdir}}

we-doc targetdir:
  just watchexec doc {{targetdir}}

we-build:
  just watchexec build

we-run-dev config:
  watchexec \
    -w target/debug/portfolio \
    --restart \
    just run {{config}}
