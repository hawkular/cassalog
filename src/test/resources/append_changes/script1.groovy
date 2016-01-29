schemaChange {
  version 'first-table'
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