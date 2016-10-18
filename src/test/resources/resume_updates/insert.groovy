schemaChange {
  version '1.0'
  cql "CREATE TABLE ${keyspace}.test (x int PRIMARY KEY, y int)"
  verify { tableExists(keyspace, "test") }
}

schemaChange {
  version '2.0'
  cql "INSERT INTO ${keyspace}.test (x, y) VALUES (1, 2)"
}