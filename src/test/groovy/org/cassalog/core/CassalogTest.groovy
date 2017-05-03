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

import org.testng.annotations.Test

import static org.testng.Assert.*

/**
 * @author jsanda
 */
class CassalogTest extends CassalogBaseTest {

  @Test
  void applySingleChangeToExistingKeyspace() {
    keyspace = 'single_change'

    resetSchema(keyspace)

    URI script = getClass().getResource('/single_change/schema.groovy').toURI()

    CassalogImpl casslog = new CassalogImpl(keyspace: keyspace, session: session)
    casslog.execute(script)

    assertTableExists(keyspace, 'test1')

    def rows = findChangeSets(keyspace, 0)

    assertEquals(rows.size(), 1, "Expected to find one row in $CassalogImpl.CHANGELOG_TABLE")
    assertEquals(rows[0].getString(0), 'first-table')
    assertNotNull(rows[0].getBytes(1))
    assertEquals(rows[0].getString(3), 'admin')
    assertEquals(rows[0].getString(4), 'test')
    assertTrue(rows[0].getSet(5, String.class).empty)

    Date appliedAt = rows[0].getTimestamp(2)
    assertNotNull(appliedAt)

    // Now running again should be a no op
    casslog.execute(script)

    rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 1, "Expected to find one row in $CassalogImpl.CHANGELOG_TABLE")
    assertEquals(rows[0].getTimestamp(2), appliedAt)
  }

  @Test
  void executeScriptThatCreatesKeyspace() {
    def keyspace = 'cassalog_create_ks'
    def change1Id = 'create-cassalog_dev'
    def change2Id = 'first-table'

    session.execute("DROP KEYSPACE IF EXISTS $keyspace")

    def script = getClass().getResource('/create_keyspace/script1.groovy').toURI()

    CassalogImpl cassalog = new CassalogImpl(session: session)
    cassalog.execute(script, [keyspace: keyspace, id1: change1Id])


    def updatedScript = getClass().getResource('/create_keyspace/script2.groovy').toURI()
    cassalog.execute(updatedScript, [keyspace: keyspace, id1: change1Id, id2: change2Id])

    assertTableExists(keyspace, 'test1')

    def rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 2)
    assertChangeSetEquals(rows[0], new ChangeSet(version: 'create-cassalog_dev', author: 'admin',
        description: 'create keyspace test'))
    assertChangeSetEquals(rows[1], new ChangeSet(version: 'first-table', author: 'admin', description: 'test'))
  }

  @Test
  void createAndUseKeyspace() {
    def keyspace = 'cassalog_create_use_ks'
    def change1Id = 'keyspsace-test'
    def change2Id = 'table-test'

    session.execute("DROP KEYSPACE IF EXISTS $keyspace")

    def script = getClass().getResource('/create_keyspace/script3.groovy').toURI()

    def cassalog = new CassalogImpl(session: session)
    cassalog.execute(script, [keyspace: keyspace, change1Id: change1Id, change2Id: change2Id])

    assertTableExists(keyspace, 'test1')

    def rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 2)
    assertChangeSetEquals(rows[0], new ChangeSet(version: change1Id, author: 'admin', description: 'create keyspace test'))
    assertChangeSetEquals(rows[1], new ChangeSet(version: change2Id, author: 'admin', description: 'test'))
  }

  @Test
  void createKeyspaceWithParameters() {
    def keyspace = 'cassalog_create_use_ks'

    session.execute("DROP KEYSPACE IF EXISTS $keyspace")

    def script = getClass().getResource('/create_keyspace/script6.groovy').toURI()

    def cassalog = new CassalogImpl(session: session)
    def replicationFactor = 3
    cassalog.execute(script, [keyspace: keyspace, replicationFactor: replicationFactor])

    def theKeyspace = findKeyspace(keyspace)
    assertTrue(theKeyspace.isPresent())
    assertEquals(theKeyspace.get().replication["replication_factor"].toInteger(), replicationFactor)

    def rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 1)
    assertChangeSetEquals(rows[0], new ChangeSet(version: '1', author: 'admin',
            description: 'create keyspace test'))
  }

  @Test(expectedExceptions = [ChangeSetException])
  void createAndDoNotUseKeyspace() {
    def keyspace = 'cassalog_do_not_use_ks'
    def change1Id = 'keyspsace-test'
    def change2Id = 'table-test'

    session.execute("DROP KEYSPACE IF EXISTS $keyspace")

    def script = getClass().getResource('/create_keyspace/script4.groovy').toURI()

    def cassalog = new CassalogImpl(session: session)
    cassalog.execute(script, [keyspace: keyspace, change1Id: change1Id, change2Id: change2Id])
  }

  @Test
  void recreateKeyspace() {
    def keyspace = 'cassalog_recreate_ks'
    def id1 = 'recreate-test'

    def script = getClass().getResource('/create_keyspace/script5.groovy').toURI()

//    def cluster2 = new Cluster.Builder().addContactPoint('127.0.0.1').build()
//    def session2 = cluster2.connect()

    def cassalog = new CassalogImpl(session: session)
    cassalog.execute(script, [keyspace: keyspace, id1: id1])

    def rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 1)

    Date dateApplied = rows[0].getTimestamp(2)

    // We will rerun the script and since the recreate attribute is set to true, Cassalog should drop the keyspace
    // and recreate it. This of course will result in the change log table being recreated. We should still only
    // have a single row in the change log. The timestamp should be later though.
    cassalog.execute(script, [keyspace: keyspace, id1: id1])

    rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 1)

    assertTrue(dateApplied < rows[0].getTimestamp(2))
  }

  @Test
  void appendChangesToExistingScript() {
    keyspace = 'append_changes'

    resetSchema(keyspace)

    def script1 = getClass().getResource("/append_changes/script1.groovy").toURI()
    def script2 = getClass().getResource("/append_changes/script2.groovy").toURI()

    CassalogImpl cassalog = new CassalogImpl(keyspace: keyspace, session: session)

    cassalog.execute(script1, [keyspace: keyspace])
    cassalog.execute(script2, [keyspace: keyspace])

    assertTableExists(keyspace, 'test2')

    def resultSet = session.execute(
        "SELECT version, hash, applied_at, author, description, tags FROM ${keyspace}.$CassalogImpl.CHANGELOG_TABLE " +
        "WHERE bucket = 0"
    )
    def rows = resultSet.all()

    assertEquals(rows.size(), 2, "Expected to find two rows in $CassalogImpl.CHANGELOG_TABLE")
    assertEquals(rows[0].getString(0), 'first-table')
    assertEquals(rows[1].getString(0), 'second-table')
    assertNotNull(rows[1].getBytes(1))
    assertEquals(rows[1].getString(3), 'admin')
    assertEquals(rows[1].getString(4), 'second table test')
    assertEquals(rows[1].getSet(5, String.class), ['red', 'blue'] as Set)
  }

  @Test
  void applyChangesAcrossMultipleBuckets() {
    keyspace = 'multiple_buckets'

    resetSchema(keyspace)

    def script1 = getClass().getResource('/multiple_buckets/script1.groovy').toURI()
    def script2 = getClass().getResource('/multiple_buckets/script2.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session, bucketSize: 2)
    cassalog.execute(script1, [keyspace: keyspace])
    cassalog.execute(script2)

    assertTableExists(keyspace, 'test3')

    // Now let's rerun the schema change script to make sure it is a no-op. Doing this will verifyFunction that we are searching
    // across buckets for changes that have already been applied.
    cassalog.execute(script2, [keyspace: keyspace])
  }

  @Test(expectedExceptions = ChangeSetAlteredException)
  void modifyingAppliedChangeSetShouldFail() {
    keyspace = 'fail_modified_changeset'

    resetSchema(keyspace)

    def script = getClass().getResource('/fail_modified_changeset/script1.groovy').toURI()
    def modifiedScript = getClass().getResource('/fail_modified_changeset/script2.groovy').toURI()

    CassalogImpl cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.execute(script, [keyspace: keyspace])
    cassalog.execute(modifiedScript, [keyspace: keyspace])

    assertTableDoesNotExist(keyspace, 'test2')

    def rows = findChangeSets(keyspace, 0)

    assertEquals(rows.size(), 1)
    assertEquals(rows[0].getString(0), 'first-table')
  }

  @Test(dependsOnMethods = 'modifyingAppliedChangeSetShouldFail')
  void abortAfterFailure() {
    keyspace = 'fail_modified_changeset'

    assertTableDoesNotExist(keyspace, 'test3')
  }

  @Test(expectedExceptions = ChangeSetAlteredException)
  void changingIdOfAppliedChangeSetShouldFail() {
    keyspace = 'change_id'

    resetSchema(keyspace)
    
    def script = getClass().getResource('/change_id/script1.groovy').toURI()
    def modifiedScript = getClass().getResource('/change_id/script2.groovy').toURI()

    def casslog = new CassalogImpl(keyspace: keyspace, session: session)
    casslog.execute(script, [keyspace: keyspace])
    casslog.execute(modifiedScript, [keyspace: keyspace])
  }

  @Test
  void setUpBasicValidation() {
    resetSchema('basic_validation')
  }

  @Test(expectedExceptions = ChangeSetValidationException, dependsOnMethods = 'setUpBasicValidation')
  void versionIsRequired() {
    keyspace = 'basic_validation'

    def script = getClass().getResource('/basic_validation/script1.groovy').toURI()
    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.execute(script, [keyspace: keyspace])
  }

  @Test(expectedExceptions = ChangeSetValidationException, dependsOnMethods = 'setUpBasicValidation')
  void cqlIsRequired() {
    keyspace = 'basic_validation'

    def script = getClass().getResource('/basic_validation/script2.groovy').toURI()
    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.execute(script, [keyspace: keyspace])
  }

  @Test
  void applyChangesWithTag() {
    keyspace = 'tags_test'
    resetSchema(keyspace)

    def script = getClass().getResource('/tags_test/script1.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.execute(script, ['dev'], [keyspace: keyspace])

    def changeLogRows = findChangeSets(keyspace, 0)

    def verifyChangeLog = { rows ->
      assertEquals(rows.size(), 2)
      assertChangeSetEquals(rows[0], new ChangeSet(version: 'table-1', author: 'admin', tags: ['dev', 'stage']))
      assertChangeSetEquals(rows[1], new ChangeSet(version: 'dev-data', author: 'admin', tags: ['dev']))
    }
    verifyChangeLog(changeLogRows)

    cassalog.execute(script, ['dev'], [keyspace: keyspace])

    // Make sure that the change log has not been altered
    changeLogRows = findChangeSets(keyspace, 0)
    verifyChangeLog(changeLogRows)
  }


  @Test
  void resumeCreateTableWhenChangeWasAlreadyApplied() {
    keyspace = 'resume_create_table_applied'
    resetSchema(keyspace)

    String cql = "CREATE TABLE ${keyspace}.test1 (x int, y text, PRIMARY KEY (x))"

    def script = getClass().getResource('/resume_updates/create_tables.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.createChangeLogTableIfNecessary()
    cassalog.initPreparedStatements()

    def changeSet = new CqlChangeSet(cql: [cql], version: '1.0')
    cassalog.insertChangeSet(changeSet, 0)

    cassalog.execute(script, [keyspace: keyspace])

    assertTableExists(keyspace, 'test1')
    assertTableExists(keyspace, 'test2')
  }

  @Test
  void resumeCreateTableWhenChangeHasNotBeenApplied() {
    keyspace = 'resume_create_table_not_applied'
    resetSchema(keyspace)

    String cql = "CREATE TABLE ${keyspace}.test1 (x int, y text, PRIMARY KEY (x))"

    def script = getClass().getResource('/resume_updates/create_tables.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.createChangeLogTableIfNecessary()
    cassalog.initPreparedStatements()

    def changeSet = new CqlChangeSet(cql: [cql], version: '1.0')
    cassalog.insertChangeSet(changeSet, 0)

    cassalog.execute(script, [keyspace: keyspace])

    assertTableExists(keyspace, 'test1')
    assertTableExists(keyspace, 'test2')
  }

  @Test
  void resumeAddColumnWhenChangeWasAlreadyApplied() {
    keyspace = 'resume_add_column_applied'
    resetSchema(keyspace)

    session.execute("CREATE TABLE ${keyspace}.test (x int, y text, PRIMARY KEY (x))")

    def script = getClass().getResource('/resume_updates/columns.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.createChangeLogTableIfNecessary()
    cassalog.initPreparedStatements()

    def addColumn = new CqlChangeSet(cql: ["ALTER TABLE ${keyspace}.test ADD z text"], version: '1.0')

    cassalog.insertChangeSet(addColumn, 0)

    cassalog.execute(script, [keyspace: keyspace])

    // No assertions necessary, just want to make sure cassalog runs without error
  }

  @Test
  void resumeAddColumnWhenChangeHasNotBeenApplied() {
    keyspace = 'resume_add_column_not_applied'
    resetSchema(keyspace)

    session.execute("CREATE TABLE ${keyspace}.test (x int, y text, PRIMARY KEY (x))")

    def script = getClass().getResource('/resume_updates/columns.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.createChangeLogTableIfNecessary()
    cassalog.initPreparedStatements()

    def addColumn = new CqlChangeSet(cql: ["ALTER TABLE ${keyspace}.test ADD z text"], version: '1.0')

    cassalog.insertChangeSet(addColumn, 0)
    cassalog.execute(script, [keyspace: keyspace])

    assertTrue(verificationFunctions.columnExists(keyspace, 'test', 'z'))
  }

  @Test
  void resumeDropColumnWhenChangeHasBeenApplied() {
    keyspace = 'resume_drop_column_applied'
    resetSchema(keyspace)

    session.execute("CREATE TABLE ${keyspace}.test (x int, y text, PRIMARY KEY (x))")

    def script = getClass().getResource('/resume_updates/columns.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.createChangeLogTableIfNecessary()
    cassalog.initPreparedStatements()

    def addColumn = new CqlChangeSet(cql: ["ALTER TABLE ${keyspace}.test ADD z text"], version: '1.0')
    def dropColumn = new CqlChangeSet(cql: ["ALTER TABLE ${keyspace}.test DROP y"], version: '2.0')

    cassalog.applyChangeSet(addColumn, 0, true)
    cassalog.insertChangeSet(dropColumn, 1)
    cassalog.execute(script, [keyspace: keyspace])

    assertTrue(verificationFunctions.columnDoesNotExist(keyspace, 'test', 'y'))
  }

  @Test
  void resumeInsertRowWhenChangeHasNotBeenApplied() {
    keyspace = 'resume_insert_row_not_applied'
    resetSchema(keyspace)

    def script = getClass().getResource('/resume_updates/insert.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.createChangeLogTableIfNecessary()
    cassalog.initPreparedStatements()

    def createTable = new CqlChangeSet(cql: ["CREATE TABLE ${keyspace}.test (x int PRIMARY KEY, y int)"],
        version: '1.0')
    cassalog.applyChangeSet(createTable, 0, true)
    def insert = new CqlChangeSet(cql: ["INSERT INTO ${keyspace}.test (x, y) VALUES (1, 2)"], version: '2.0')
    cassalog.insertChangeSet(insert, 1)
    cassalog.execute(script, [keyspace: keyspace])

    def resultSet = session.execute("select y from ${keyspace}.test where x = 1")
    def rows = resultSet.all()
    assertEquals(rows.size(), 1)
    assertEquals(2 as Integer, rows[0].getInt(0))
  }
}
