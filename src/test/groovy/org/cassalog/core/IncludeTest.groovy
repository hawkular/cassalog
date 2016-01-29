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

import static org.testng.Assert.assertEquals

/**
 * @author jsanda
 */
class IncludeTest extends CassalogBaseTest {

  @Test
  void includeScriptAfterChanges() {
    def keyspace = 'include_after'
    resetSchema(keyspace)

    def script = getClass().getResource('/include/script1.groovy').toURI()

    def cassalog = new Cassalog(keyspace: keyspace, session: session)
    cassalog.execute(script, [keyspace: keyspace])

    assertTableExists(keyspace, 'test1')
    assertTableExists(keyspace, 'test2')

    def rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 2)
    assertChangeSetEquals(rows[0], new ChangeSet(version: 'first-table'))
    assertChangeSetEquals(rows[1], new ChangeSet(version: 'second-table'))
  }

  @Test
  void includeScriptBeforeChanges() {
    def keyspace = 'include_before'
    resetSchema(keyspace)

    def script = getClass().getResource('/include/script3.groovy').toURI()

    def cassalog = new Cassalog(keyspace: keyspace, session: session)
    cassalog.execute(script, [keyspace: keyspace])

    def rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 2)
    assertChangeSetEquals(rows[0], new ChangeSet(version: 'second-table'))
    assertChangeSetEquals(rows[1], new ChangeSet(version: 'third-table'))
  }

  @Test
  void multipleIncludes() {
    def keyspace = 'multiple_includes'
    resetSchema(keyspace)

    def script = getClass().getResource('/include/multiple_includes.groovy').toURI()

    def cassalog = new Cassalog(keyspace: keyspace, session: session)
    cassalog.execute(script, [keyspace: keyspace])

    def rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 2)
    assertChangeSetEquals(rows[0], new ChangeSet(version: 'second-table'))
    assertChangeSetEquals(rows[1], new ChangeSet(version: 'fourth-table'))
  }

  @Test
  void nestedIncludes() {
    def keyspace = 'nested_includes'
    resetSchema(keyspace)

    def script = getClass().getResource('/include/nested.groovy').toURI()

    def cassalog = new Cassalog(keyspace: keyspace, session: session)
    cassalog.execute(script, [keyspace: keyspace])

    def rows = findChangeSets(keyspace, 0)
    assertEquals(rows.size(), 2)
    assertChangeSetEquals(rows[0], new ChangeSet(version: 'second-table'))
    assertChangeSetEquals(rows[1], new ChangeSet(version: 'third-table'))
  }

}
