DROP SCHEMA etl;

CREATE SCHEMA etl;

DROP TABLE etl_snapshoot;

CREATE TABLE etl.etl_snapshoot(
    trg_table_name varchar(200) not null,
    src_index_name varchar(200) not null,
	src_pk_col_name varchar(100),
    src_timestamp_col_name varchar(100),
    last_sync_timestamp timestamp without time zone,
	is_active boolean DEFAULT TRUE,
    primary key (trg_table_name)
);

INSERT INTO etl.etl_snapshoot(trg_table_name, src_index_name, src_pk_col_name, src_timestamp_col_name, last_sync_timestamp)
VALUES('mapservice_geolocations','mapservice-geolocations','placeId', 'updatedAt','1900-01-01 00:00:00');

SELECT * FROM etl.etl_snapshoot;