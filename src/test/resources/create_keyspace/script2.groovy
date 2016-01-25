createKeyspace {
  id id1
  name keyspace
  author 'admin'
  description 'create keyspace test'
  replication {
    replication_factor 2
  }
}

schemaChange {
  id id2
  author 'admin'
  description 'test'
  cql """
    CREATE TABLE cassalog_dev.test1 (
        x int,
        y text,
        z text,
        PRIMARY KEY (x, y)
    )
   """
}