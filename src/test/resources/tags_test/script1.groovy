schemaChange {
  id 'table-1'
  author 'admin'
  cql """
CREATE TABLE ${keyspace}.test1 (
    x int,
    y int,
    PRIMARY KEY (x)
)
"""
}

schemaChange {
  id 'dev-data'
  author 'admin'
  tags 'dev'
  cql "INSERT INTO ${keyspace}.test1 (x, y) VALUES (1, 2)"
}

schemaChange {
  id 'stage-data'
  author 'admin'
  tags 'stage'
  cql "INSERT INTO ${keyspace}.test1 (x, y) VALUES (2, 3)"
}