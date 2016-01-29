schemaChange {
  version 'first-table'
  author 'admin'
  description 'test'
  cql """
    CREATE TABLE ${keyspace}.test1(
        x1 int,
        y1 text,
        z1 text,
        PRIMARY KEY(x1, y1)
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
        PRIMARY KEY(x2, y2)
    )
    """
}

schemaChange {
  version 'third-table'
  author 'admin'
  description '3rd table'
  tags '1', '2', '3'
  cql """
      CREATE TABLE ${keyspace}.test3 (
        x3 text,
        y3 int,
        z3 text,
        PRIMARY KEY (x3, z3)
      )
    """
}

