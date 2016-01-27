include '/include/script2.groovy'

schemaChange {
  id 'third-table'
  cql """
    CREATE TABLE ${keyspace}.test3 (
      x int,
      y text,
      z text,
      PRIMARY KEY (y, z)
    )
"""
}