CREATE TABLE cursor (
    site text DEFAULT '' NOT NULL,
    source text NOT NULL,
    cursor text DEFAULT '' NOT NULL
);

ALTER TABLE ONLY cursor
    ADD CONSTRAINT cursor_pkey PRIMARY KEY (site, source);
