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

// This change will fail if the bootstrap callback does not execute
schemaChange {
    version 'second-table'
    cql "DROP TABLE ${keyspace}.test2"
}