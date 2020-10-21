CREATE TABLE tlog (
    t_id bigserial PRIMARY KEY,
    t_timestamp timestamp NOT NULL,
    t_server_id varchar(64) NOT NULL,
    t_unique_name varchar(64) NOT NULL,
    t_gtrid bytea,
    t_bqual bytea,
    t_status boolean NOT NULL,
    t_complete boolean NOT NULL
);