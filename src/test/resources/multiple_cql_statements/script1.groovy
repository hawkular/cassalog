setKeyspace keyspace

schemaChange {
  version '1.0'
  cql """
CREATE TABLE test (
      x int,
      y text,
      PRIMARY KEY (x)
    )
"""
}

schemaChange {
  version '1.1'
  cql ([
      "INSERT INTO test (x, y) VALUES (0, '1')",
      "INSERT INTO test (x, y) VALUES (1, '2')",
      "INSERT INTO test (x, y) VALUES (2, '3')"
  ])
}