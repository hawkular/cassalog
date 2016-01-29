schemaChange {
  version 'table-1'
  author 'admin'
  description 'test 1'
  cql """
    CREATE TABLE ${keyspace}.test1 (
      x int,
      y int,
      PRIMARY KEY (x)
    )
  """
}