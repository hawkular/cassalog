setKeyspace keyspace

schemaChange {
  version 'first-table'
  cql """
    CREATE TABLE always_run (
      x int,
      y text,
      z text,
      PRIMARY KEY (x, y)
    )
"""
}
