bootstrap {
    session.execute("CREATE TABLE ${keyspace}.test2 (x text PRIMARY KEY)")
}

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

schemaChange {
    version 'second-table'
    cql "DROP TABLE ${keyspace}.test2"
}

bootstrap {
    throw new RuntimeException("This should not execute")
}