setKeyspace keyspace

schemaChange {
  id 'first-table'
  cql """
    CREATE TABLE test (
      x int,
      y text,
      z text,
      PRIMARY KEY (x, y)
    )
"""
}