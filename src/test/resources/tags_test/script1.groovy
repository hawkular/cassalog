schemaChange {
  version 'table-1'
  author 'admin'
  tags 'dev', 'stage'
  cql """
CREATE TABLE ${keyspace}.test1 (
    x int,
    y int,
    PRIMARY KEY (x)
)
"""
}

schemaChange {
  version 'dev-data'
  author 'admin'
  tags 'dev'
  cql "INSERT INTO ${keyspace}.test1 (x, y) VALUES (1, 2)"
}

schemaChange {
  version 'stage-data'
  author 'admin'
  tags 'stage'
  cql "INSERT INTO ${keyspace}.test1 (x, y) VALUES (2, 3)"
}