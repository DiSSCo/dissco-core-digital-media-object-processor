CREATE TABLE public.new_digital_media_object (
	id text NOT NULL,
	"version" int4 NOT NULL,
	"type" text NULL,
	digital_specimen_id text NOT NULL,
	media_url text NULL,
	format text NULL,
	source_system_id text NOT NULL,
	created timestamptz NOT NULL,
	last_checked timestamptz NOT NULL,
	deleted timestamptz NULL,
	"data" jsonb NULL,
	original_data jsonb NULL,
	CONSTRAINT new_digital_media_object_pkey PRIMARY KEY (id)
);
CREATE INDEX new_digital_media_object_id_idx ON public.new_digital_media_object USING btree (id, version);
CREATE UNIQUE INDEX new_digital_media_object_id_version_url ON public.new_digital_media_object USING btree (id, version, media_url);

CREATE TABLE public.handles (
	handle bytea NOT NULL,
	idx int4 NOT NULL,
	"type" bytea NULL,
	"data" bytea NULL,
	ttl_type int2 NULL,
	ttl int4 NULL,
	"timestamp" int8 NULL,
	refs text NULL,
	admin_read bool NULL,
	admin_write bool NULL,
	pub_read bool NULL,
	pub_write bool NULL,
	CONSTRAINT handles_pkey PRIMARY KEY (handle, idx)
);
CREATE INDEX dataindex ON public.handles USING btree (data);
CREATE INDEX handleindex ON public.handles USING btree (handle);