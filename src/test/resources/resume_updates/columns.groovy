schemaChange {
  version '1.0'
  cql "ALTER TABLE ${keyspace}.test ADD z text"
  verify { columnExists(keyspace, "test", "z") }
}

schemaChange {
  version '2.0'
  cql "ALTER TABLE ${keyspace}.test DROP y"
  verify { columnDoesNotExist(keyspace, "test", "z") }
}