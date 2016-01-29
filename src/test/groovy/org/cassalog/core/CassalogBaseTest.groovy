/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cassalog.core
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import org.testng.annotations.BeforeSuite

import static org.testng.Assert.*
/**
 * @author jsanda
 */
class CassalogBaseTest {

  static Session session

  static PreparedStatement findTableName

  static Cluster cluster

  @BeforeSuite
  static void initTest() {
    cluster = new Cluster.Builder().addContactPoint('127.0.0.1').build()
    session = cluster.connect()
    findTableName = session.prepare(
        "SELECT columnfamily_name FROM system.schema_columnfamilies " +
        "WHERE keyspace_name = ? AND columnfamily_name = ?"
    )
  }

  static void resetSchema(String keyspace) {
    session.execute("DROP KEYSPACE IF EXISTS $keyspace")
    session.execute(
        "CREATE KEYSPACE $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
  }

  static def findChangeSets(keyspace, bucket) {
    def resultSet = session.execute(
        "SELECT version, hash, applied_at, author, description, tags FROM ${keyspace}.$Cassalog.CHANGELOG_TABLE " +
            "WHERE bucket = $bucket"
    )
    return resultSet.all()
  }

  static void assertChangeSetEquals(Row actual, ChangeSet expected) {
    assertEquals(actual.getString(0), expected.version)
    assertNotNull(actual.getBytes(1))
    assertNotNull(actual.getTimestamp(2))
    assertEquals(actual.getString(3), expected.author)
    assertEquals(actual.getString(4), expected.description)
    assertEquals(actual.getSet(5, String), expected.tags)
  }

  static void assertTableExists(String keyspace, String table) {
    def resultSet = session.execute(findTableName.bind(keyspace, table))
    assertFalse(resultSet.exhausted, "The table ${keyspace}.$table does not exist")
  }

  static void assertTableDoesNotExist(String keyspace, String table) {
    def resultSet = session.execute(findTableName.bind(keyspace, table))
    assertTrue(resultSet.exhausted, "The table ${keyspace}.$table exists")
  }

}
