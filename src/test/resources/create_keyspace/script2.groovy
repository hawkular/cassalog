createKeyspace {
  version id1
  name keyspace
  author 'admin'
  description 'create keyspace test'
  replication {
    replication_factor 2
  }
}

schemaChange {
  version id2
  author 'admin'
  description 'test'
  cql """
    CREATE TABLE ${keyspace}.test1 (
        x int,
        y text,
        z text,
        PRIMARY KEY (x, y)
    )
   """
}