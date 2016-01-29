include '/include/script2.groovy'

schemaChange {
  version 'third-table'
  cql """
    CREATE TABLE ${keyspace}.test3 (
      x int,
      y text,
      z text,
      PRIMARY KEY (y, z)
    )
"""
}