create table digital_media_object
(
    id text not null
        constraint digital_media_object_pk
            primary key,
    version integer not null,
    type text,
    digital_specimen_id text not null,
    media_url text not null,
    created timestamp with time zone not null,
    last_checked timestamp with time zone not null,
    deleted timestamp with time zone,
    data jsonb not null,
    original_data jsonb not null
);

create index digital_media_object_id_idx
    on digital_media_object (id, media_url);

create unique index digital_media_object_id_version_url
    on digital_media_object (id, version, media_url);

create index digital_media_object_digital_specimen_id_url
    on digital_media_object (digital_specimen_id, media_url);
