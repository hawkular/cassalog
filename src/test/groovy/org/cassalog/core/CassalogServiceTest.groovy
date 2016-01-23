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
import org.testng.Assert
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test

import static org.testng.Assert.*
/**
 * @author jsanda
 */
class CassalogServiceTest {

  static Session session

  static PreparedStatement findTableName

  @BeforeClass
  static void initTest() {
    Cluster cluster = new Cluster.Builder().addContactPoint('127.0.0.1').build()
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

  @Test
  void executeScriptWithSingleChange() {
    String keyspace = 'single_change'

    resetSchema(keyspace)

    String table = 'test1'
    def script = """
schemaChange {
  id 'first-table'
  author 'admin'
  description 'test'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test1 (
      x int,
      y text,
      z text,
      PRIMARY KEY (x, y)
    )
\"\"\"
}
"""
    CassalogService casslog = new CassalogService(keyspace: keyspace, session: session)
    casslog.execute(new StringReader(script))

    assertTableExists(keyspace, table)

    def resultSet = session.execute(
        "SELECT id, hash, applied_at, author, description, tags FROM ${keyspace}.$CassalogService.CHANGELOG_TABLE " +
        "WHERE bucket = 0"
    )
    def rows = resultSet.all()

    Assert.assertEquals(rows.size(), 1, "Expected to find one row in $CassalogService.CHANGELOG_TABLE")
    Assert.assertEquals(rows[0].getString(0), 'first-table')
    org.testng.Assert.assertNotNull(rows[0].getBytes(1))
    Assert.assertEquals(rows[0].getString(3), 'admin')
    Assert.assertEquals(rows[0].getString(4), 'test')
    assertTrue(rows[0].getSet(5, String.class).empty)

    Date appliedAt = rows[0].getTimestamp(2)
    org.testng.Assert.assertNotNull(appliedAt)

    // Now running again should be a no op
    casslog.execute(new StringReader(script))

    resultSet = session.execute(
       "SELECT id, hash, applied_at, author, description, tags FROM ${keyspace}.$CassalogService.CHANGELOG_TABLE " +
       "WHERE bucket = 0"
    )
    rows = resultSet.all()

    Assert.assertEquals(rows.size(), 1, "Expected to find one row in $CassalogService.CHANGELOG_TABLE")
    assertEquals(rows[0].getTimestamp(2), appliedAt)
  }

  @Test
  void appendChangesToExistingScript() {
    String keyspace = 'append_changes'

    resetSchema(keyspace)

    def script = """
schemaChange {
  id 'first-table'
  author 'admin'
  description 'test'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test1 (
      x int,
      y text,
      z text,
      PRIMARY KEY (x, y)
    )
\"\"\"
}
"""

    def updatedScript = script + "\n\n" + """
schemaChange {
  id 'second-table'
  author 'admin'
  description 'second table test'
  tags 'red', 'blue'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test2 (
        x2 int,
        y2 text,
        z2 text,
        PRIMARY KEY (x2, y2)
    )
    \"\"\"
}
"""
    CassalogService casslog = new CassalogService(keyspace: keyspace, session: session)
    casslog.execute(new StringReader(updatedScript))

    assertTableExists(keyspace, 'test2')

    def resultSet = session.execute(
        "SELECT id, hash, applied_at, author, description, tags FROM ${keyspace}.$CassalogService.CHANGELOG_TABLE " +
        "WHERE bucket = 0"
    )
    def rows = resultSet.all()

    assertEquals(rows.size(), 2, "Expected to find two rows in $CassalogService.CHANGELOG_TABLE")
    assertEquals(rows[0].getString(0), 'first-table')
    assertEquals(rows[1].getString(0), 'second-table')
    assertNotNull(rows[1].getBytes(1))
    assertEquals(rows[1].getString(3), 'admin')
    assertEquals(rows[1].getString(4), 'second table test')
    assertEquals(rows[1].getSet(5, String.class), ['red', 'blue'] as Set)
  }

  @Test
  void applyChangesAcrossMultipleBuckets() {
    String keyspace = 'multiple_buckets'

    resetSchema(keyspace)

    def script = """
schemaChange {
  id 'first-table'
  author 'admin'
  description 'test'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test1(
        x1 int,
        y1 text,
        z1 text,
        PRIMARY KEY(x1, y1)
    )
    \"\"\"
  }

  schemaChange {
    id 'second-table'
    author 'admin'
    description 'second table test'
    tags 'red', 'blue'
    cql \"\"\"
    CREATE TABLE ${keyspace}.test2 (
        x2 int,
        y2 text,
        z2 text,
        PRIMARY KEY(x2, y2)
    )
    \"\"\"
}
"""
    def casslog = new CassalogService(keyspace: keyspace, session: session, bucketSize: 2)
    casslog.execute(new StringReader(script))

    def updatedScript = script + "\n\n" + """
  schemaChange {
    id 'third-table'
    author 'admin'
    description '3rd table'
    tags '1', '2', '3'
    cql \"\"\"
      CREATE TABLE ${keyspace}.test3 (
        x3 text,
        y3 int,
        z3 text,
        PRIMARY KEY (x3, z3)
      )
    \"\"\"
  }
"""
    casslog.execute(new StringReader(updatedScript))

    assertTableExists(keyspace, 'test3')

    // Now let's rerun the schema change script to make sure it is a no-op. Doing this will verify that we are searching
    // across buckets for changes that have already been applied.
    casslog.execute(new StringReader(updatedScript))
  }

  @Test(expectedExceptions = ChangeSetAlteredException)
  void modifyingAppliedChangeSetShouldFail() {
    String keyspace = 'fail_modified_changeset'

    resetSchema(keyspace)

    def script = """
schemaChange {
  id 'first-table'
  author 'admin'
  description 'test 1'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test1 (
        x1 int,
        y1 text,
        z1 text,
        PRIMARY KEY (x1, y1)
    )
    \"\"\"
}
"""
    CassalogService casslog = new CassalogService(keyspace: keyspace, session: session)
    casslog.execute(new StringReader(script))

    def modifiedScript = """
schemaChange {
  id 'first-table'
  author 'admin'
  description 'test 1'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test1 (
      x1 int,
      y1 text,
      z1 text,
      PRIMARY KEY (x1, z1)
    )
  \"\"\"
}

schemaChange {
  id 'second-table'
  author 'admin'
  description 'test 2'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test2 (
      x2 text,
      y2 text,
      PRIMARY KEY (x2)
    )
  \"\"\"
}

schemaChange {
  id 'third-table'
  author 'admin'
  description 'test 3'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test3 (
      x3 int,
      y3 text,
      PRIMARY KEY (x3)
    )
  \"\"\"
}
"""
    casslog.execute(new StringReader(modifiedScript))

    assertTableDoesNotExist(keyspace, 'test2')

    def resultSet = session.execute(
        "SELECT id, hash, applied_at, author, description, tags FROM ${keyspace}.$CassalogService.CHANGELOG_TABLE " +
        "WHERE bucket = 0"
    )
    def rows = resultSet.all()

    assertEquals(rows.size(), 1)
    assertEquals(rows[0].getString(0), 'first-table')
  }

  @Test(dependsOnMethods = 'modifyingAppliedChangeSetShouldFail')
  void abortAfterFailure() {
    String keyspace = 'fail_modified_changeset'

    assertTableDoesNotExist(keyspace, 'test3')
  }

  @Test(expectedExceptions = ChangeSetAlteredException)
  void changingIdOfAppliedChangeSetShouldFail() {
    String keyspace = 'change_id'

    resetSchema(keyspace)
    
    def script = """
schemaChange {
  id 'first-table'
  author 'admin'
  description 'test 1'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test1 (
      x int,
      y int,
      PRIMARY KEY (x)
    )
  \"\"\"  
}
"""
    def casslog = new CassalogService(keyspace: keyspace, session: session)
    casslog.execute(new StringReader(script))

    def modifiedScript = """
schemaChange {
  id 'table-1'
  author 'admin'
  description 'test 1'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test1 (
      x int,
      y int,
      PRIMARY KEY (x)
    )
  \"\"\"
}
"""
    casslog.execute(new StringReader(modifiedScript))
  }

  @Test
  void setUpBasicValidation() {
    resetSchema('basic_validation')
  }

  @Test(expectedExceptions = ChangeSetValidationException, dependsOnMethods = 'setUpBasicValidation')
  void idIsRequired() {
    String keyspace = 'basic_validation'

    def script = """
schemaChange {
  author 'admin'
  cql \"\"\"
    CREATE TABLE ${keyspace}.test (
      x int,
      y int,
      PRIMARY KEY (x)
    )
  \"\"\"
}
"""
    def cassalog = new CassalogService(keyspace: keyspace, session: session)
    cassalog.execute(new StringReader(script))
  }

  @Test(expectedExceptions = ChangeSetValidationException, dependsOnMethods = 'setUpBasicValidation')
  void cqlIsRequired() {
    String keyspace = 'basic_validation'

    def script = """
schemaChange {
  id 'no-cql'
  author 'admin'
}
"""
    def cassalog = new CassalogService(keyspace: keyspace, session: session)
    cassalog.execute(new StringReader(script))
  }

  @Test
  void applyChangesWithTag() {
    String keyspace = 'tags_test'
    resetSchema(keyspace)

    def change1Cql = """
CREATE TABLE ${keyspace}.test1 (
    x int,
    y int,
    PRIMARY KEY (x)
)
"""
    def change2Cql = "INSERT INTO ${keyspace}.test1 (x, y) VALUES (1, 2)"
    def change3Cql = "INSERT INTO ${keyspace}.test1 (x, y) VALUES (2, 3)"

    def script = """
schemaChange {
  id 'table-1'
  author 'admin'
  cql \"\"\"$change1Cql\"\"\"
}

schemaChange {
  id 'dev-data'
  author 'admin'
  tags 'dev'
  cql "$change2Cql"
}

schemaChange {
  id 'stage-data'
  author 'admin'
  tags 'stage'
  cql "$change3Cql"
}
"""
    def cassalog = new CassalogService(keyspace: keyspace, session: session)
    def computeHash = cassalog.&computeHash
    cassalog.execute(new StringReader(script), ['dev'])

    def rows = findChangeSets(keyspace, 0)

    assertEquals(rows.size(), 2)
    assertChangeSetEquals(rows[0], new ChangeSet(id: 'table-1', author: 'admin', hash: computeHash(change1Cql)))
    assertChangeSetEquals(rows[1], new ChangeSet(id: 'dev-data', author: 'admin', hash: computeHash(change2Cql),
        tags: ['dev']))

    cassalog.execute(new StringReader(script), ['dev'])
  }

  static def findChangeSets(keyspace, bucket) {
    def resultSet = session.execute(
        "SELECT id, hash, applied_at, author, description, tags FROM ${keyspace}.$CassalogService.CHANGELOG_TABLE " +
        "WHERE bucket = $bucket"
    )
    return resultSet.all()
  }

  static void assertChangeSetEquals(Row actual, ChangeSet expected) {
    assertEquals(actual.getString(0), expected.id)
    assertEquals(actual.getBytes(1), expected.hash)
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
