-- a "registry" is a top-level store of container images, eg "meow" in
-- "registry.digitalocean.com/meow/nginx:latest"
CREATE TABLE registries (
	id SERIAL PRIMARY key,
	name VARCHAR(128)
);

-- a "repository" is the name of an image, eg "nginx" in
-- "registry.digitalocean.com/meow/nginx:latest"
CREATE TABLE repositories (
	id SERIAL PRIMARY key,
	registry INT NOT NULL REFERENCES registries (id),
	name VARCHAR(128)
);

-- a blob is a chunk of data, most likely either a manifest config file or an
-- image layer
CREATE TABLE blobs (
	id SERIAL PRIMARY key,
	digest VARCHAR(256) UNIQUE NOT NULL,
	object_key UUID NOT NULL
);

-- a manifest is an OCI image manifest:
-- https://github.com/opencontainers/image-spec/blob/main/manifest.md
CREATE TABLE manifests (
	id SERIAL PRIMARY key,
	registry INT NOT NULL REFERENCES registries (id),
	repository INT NOT NULL REFERENCES repositories (id),
	config VARCHAR(256) NOT NULL REFERENCES blobs (digest),
	digest VARCHAR(256) UNIQUE NOT NULL
);

-- an index_manifest is a reference from a parent manifest to a child manifest
CREATE TABLE index_manifests (
	parent_manifest INT NOT NULL REFERENCES manifests (id),
	child_manifest INT NOT NULL REFERENCES manifests (id)
);

-- a layer is a reference from a manifest to a blob
CREATE TABLE layers (
	manifest INT NOT NULL REFERENCES manifests (id),
	blob INT NOT NULL REFERENCES blobs (id)
);

-- a tag is a reference to a manifest
CREATE TABLE tags (
	id SERIAL PRIMARY key,
	name VARCHAR(256),
	manifest_id INT NOT NULL REFERENCES manifests (id)
);

-- http upload sessions. sessions are used to track a sequence of stateful http
-- requests, usually for chunked blob uploads.
CREATE TABLE upload_sessions (
	uuid UUID PRIMARY key DEFAULT gen_random_uuid(),
	start_date DATE NOT NULL DEFAULT now(),
	digest_state JSONB NOT NULL,
	chunk_info JSONB
);
