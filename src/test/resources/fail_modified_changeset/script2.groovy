schemaChange {
  version 'first-table'
  author 'admin'
  description 'test 1'
  cql """
    CREATE TABLE ${keyspace}.test1 (
      x1 int,
      y1 text,
      z1 text,
      PRIMARY KEY (x1, z1)
    )
  """
}