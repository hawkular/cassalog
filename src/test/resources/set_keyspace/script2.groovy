setKeyspace 'does_not_exist'

schemaChange {
  version 'first-table'
  cql """
    CREATE TABLE test (
      x int,
      y text,
      z text,
      PRIMARY KEY (x, y)
    )
"""
}