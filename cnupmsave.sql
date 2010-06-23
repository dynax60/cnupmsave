CREATE TABLE user_traf (
    dt date NOT NULL,
    ipaddr inet NOT NULL,
    inbound bigint NOT NULL,
    outbound bigint NOT NULL,
    updated timestamp without time zone
);


CREATE TABLE user_traf_dtl (
    dt date NOT NULL,
    src inet NOT NULL,
    dst inet NOT NULL,
    bytes bigint NOT NULL,
    updated time without time zone
);
