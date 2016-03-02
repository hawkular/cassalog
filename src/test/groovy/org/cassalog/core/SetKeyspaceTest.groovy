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

/**
 * @author jsanda
 */
class SetKeyspaceTest extends CassalogBaseTest {

  @Test
  void setKeyspace() {
    def keyspace = 'set_keyspace'
    resetSchema(keyspace)

    def script = getClass().getResource('/set_keyspace/script1.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.execute(script)

    assertTableExists(keyspace, 'test')
  }

  @Test(expectedExceptions = [ChangeSetException])
  setKeyspaceInvalid() {
    def script = getClass().getResource('/set_keyspace/script2.groovy').toURI()

    def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
    cassalog.execute(script)
  }

}
