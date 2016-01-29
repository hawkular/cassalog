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

schemaChange {
  version 'second-table'
  author 'admin'
  description 'second table test'
  tags 'red', 'blue'
  cql """
    CREATE TABLE ${keyspace}.test2 (
        x2 int,
        y2 text,
        z2 text,
        PRIMARY KEY (x2, y2)
    )
    """
}