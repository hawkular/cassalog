createKeyspace {
  id change1Id
  name keyspace
  // Note that the active attribute is set to false. This means that the following CREATE TABLE statement must
  // prefix the table name with the keyspace in order for the table to be created in the correct keyspace.
  active false
}

schemaChange {
  id change2Id
  cql """
    CREATE TABLE test1 (
        x int,
        y text,
        z text,
        PRIMARY KEY (x, y)
    )
   """
}