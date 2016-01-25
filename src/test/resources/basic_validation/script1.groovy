schemaChange {
  author 'admin'
  cql """
    CREATE TABLE ${keyspace}.test (
      x int,
      y int,
      PRIMARY KEY (x)
    )
  """
}