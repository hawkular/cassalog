schemaChange {
  id 'first-table'
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
    id 'second-table'
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