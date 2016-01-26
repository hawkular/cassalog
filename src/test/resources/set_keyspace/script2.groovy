setKeyspace 'does_not_exist'

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