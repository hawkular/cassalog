schemaChange {
  version 'first-table'
  cql """
    CREATE TABLE ${keyspace}.test1 (
      x int,
      y text,
      z text,
      PRIMARY KEY (x, y)
    )
"""
}

include '/include/script2.groovy'