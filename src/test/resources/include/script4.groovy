schemaChange {
  id 'fourth-table'
  cql """
    CREATE TABLE ${keyspace}.test4 (
      x int,
      y text,
      z text,
      PRIMARY KEY (y, z)
    )
"""
}