CREATE KEYSPACE IF NOT EXISTS test WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS test.tbl_name (
    pk_uuid uuid,
    invoice_date timeuuid,
    is_active boolean,
    address list<text>,
    PRIMARY KEY (pk_uuid, invoice_date)
) WITH CLUSTERING ORDER BY (invoice_date DESC);