CREATE TABLE tlog (
    t_id bigserial PRIMARY KEY,
    t_timestamp timestamp NOT NULL,
    t_server_id varchar(64) NOT NULL,
    t_gtrid bytea,
    t_status boolean NOT NULL
);