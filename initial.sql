CREATE TABLE cursor (
    site text NOT NULL,
    source text NOT NULL,
    cursor text NOT NULL,
    ts bigint
);


ALTER TABLE ONLY cursor
    ADD CONSTRAINT cursor_pkey PRIMARY KEY (site, source);
