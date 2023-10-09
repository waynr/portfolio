-- a "registry" is a top-level store of container images, eg "meow" in
-- "registry.digitalocean.com/meow/nginx:latest"
CREATE TABLE registries (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	name VARCHAR(128) UNIQUE NOT NULL
);

-- a "repository" is the name of an image, eg "nginx" in
-- "registry.digitalocean.com/meow/nginx:latest"
CREATE TABLE repositories (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	registry_id UUID NOT NULL REFERENCES registries (id),
	name VARCHAR(128) NOT NULL,
	UNIQUE (registry_id, name)
);

-- a blob is a chunk of data, most likely either a manifest config file or an
-- image layer
CREATE TABLE blobs (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	digest VARCHAR(256) NOT NULL,
	registry_id UUID NOT NULL REFERENCES registries (id),
	UNIQUE (digest, registry_id)
);

-- a manifest is an OCI image manifest:
-- https://github.com/opencontainers/image-spec/blob/main/manifest.md
CREATE TABLE manifests (
	id UUID PRIMARY KEY,
	registry_id UUID NOT NULL REFERENCES registries (id),
	repository_id UUID NOT NULL REFERENCES repositories (id),
	blob_id UUID NOT NULL REFERENCEs blobs(id),
	media_type VARCHAR(512) DEFAULT NULL,
	artifact_type VARCHAR(512) DEFAULT NULL,
	digest VARCHAR(256) NOT NULL,
	subject VARCHAR(256) DEFAULT NULL,
	UNIQUE (registry_id, repository_id, digest)
);

-- an index_manifest is a reference from a parent manifest to a child manifest
CREATE TABLE index_manifests (
	parent_manifest UUID NOT NULL REFERENCES manifests (id),
	child_manifest UUID NOT NULL REFERENCES manifests (id)
);

-- a layer is a reference from a manifest to a blob
CREATE TABLE layers (
	manifest UUID NOT NULL REFERENCES manifests (id),
	blob UUID NOT NULL REFERENCES blobs (id)
);

-- a tag is a reference to a manifest
CREATE TABLE tags (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	repository_id UUID NOT NULL REFERENCES repositories (id),
	manifest_id UUID NOT NULL REFERENCES manifests (id),
	name VARCHAR(256) NOT NULL,
	UNIQUE (repository_id, name)
);

-- http upload sessions. sessions are used to track a sequence of stateful http
-- requests, usually for chunked blob uploads.
CREATE TABLE upload_sessions (
	uuid UUID PRIMARY key DEFAULT gen_random_uuid(),
	-- POST-PUT uploads don't include a content digest ahead of time so this may
	-- not be very useful
	-- content_digest VARCHAR(256) NOT NULL
	start_date DATE NOT NULL DEFAULT now(),
	upload_id VARCHAR(256),
	chunk_number INT4 NOT NULL DEFAULT 1,
	last_range_end BIGINT NOT NULL DEFAULT 0,
	digest_state JSONB
);

-- chunk data by upload session
CREATE TABLE chunks (
	chunk_number INT4 NOT NULL,
	upload_session_uuid UUID NOT NULL REFERENCES upload_sessions(uuid),
	e_tag VARCHAR(128),
	UNIQUE (chunk_number, upload_session_uuid)
);
