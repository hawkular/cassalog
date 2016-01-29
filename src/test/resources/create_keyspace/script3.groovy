createKeyspace {
  version change1Id
  name keyspace
  author 'admin'
  description 'create keyspace test'
  replication {
    replication_factor 2
  }
}

schemaChange {
  version change2Id
  author 'admin'
  description 'test'
  // Note that the CREATE TABLE statement does not prefix the table name with the keyspace. This is because the
  // createKeyspace function by default sets the working keyspace to the one created with the USE CQL command.
  cql """
    CREATE TABLE test1 (
        x int,
        y text,
        z text,
        PRIMARY KEY (x, y)
    )
   """
}