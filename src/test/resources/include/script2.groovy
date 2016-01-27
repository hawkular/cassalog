schemaChange {
  id 'second-table'
  cql """
    CREATE TABLE ${keyspace}.test2 (
      x int,
      y text,
      z text,
      PRIMARY KEY (y, z)
    )
"""
}