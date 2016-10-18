schemaChange {
  version '1.0'
  cql "CREATE TABLE ${keyspace}.test1 (x int, y text, PRIMARY KEY (x))"
  verify { tableExists(keyspace, "test1") }
}

schemaChange {
  version '2.0'
  cql """
CREATE TABLE ${keyspace}.test2 (
      x int,
      y text,
      PRIMARY KEY (x)
    )
"""
}