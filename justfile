devenv-up:
  docker compose up -d
  just init-minio
  just init-cockroachdb
  just sqlx-migrate

devenv-down:
  docker compose down
  docker volume ls -f name=portfolio -q \
    | xargs docker volume rm

init-minio:
  docker exec -it portfolio-minio1-1 \
    bash -x /portfolio/scripts/minio-init.bash

init-cockroachdb:
  docker exec -it portfolio-roach1-1 \
    ./cockroach --host=roach1:26257 init --insecure

sqlx-migrate:
  sqlx migrate --source db/postgres/migrations run

we-build:
  watchexec \
    -c \
    -w src \
    -w Cargo.toml \
    -w justfile \
    -e toml,rs \
    'cargo build --all'
