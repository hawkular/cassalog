/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package cassalog.core
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing

import java.nio.ByteBuffer

/**
 * @author jsanda
 */
class SchemaManager {

  static final String CHANGELOG_TABLE = 'schema_changelog'

  static final int DEFAULT_BUCKET_SIZE = 50

  String keyspace

  Session session

  int bucketSize = DEFAULT_BUCKET_SIZE

  HashFunction hashFunction = Hashing.sha1()

  PreparedStatement insertSchemaChange

  void execute(Reader script) {
    createSchemaChangesTableIfNecessary()

    insertSchemaChange = session.prepare("""
      INSERT INTO ${keyspace}.$CHANGELOG_TABLE (bucket, revision, id, applied_at, hash, author, description, tags)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      """
    )

    def changeLog = new ChangeLog(session: session, keyspace: keyspace,  bucketSize: bucketSize)
    changeLog.load()

    def schemaChanges = []

    GroovyShell shell = new GroovyShell(createBinding(schemaChanges))
    shell.evaluate(script)

    schemaChanges.eachWithIndex{ ChangeSet change, int i ->
      byte[] bytes = hashFunction.hashBytes(change.cql.bytes).asBytes()
      change.hash = ByteBuffer.wrap(bytes)

      if (i < changeLog.size) {
        if (changeLog[i].id != change.id) {
          throw new ChangeSetException("The id [$change.id] for $change does not match the id " +
              "[${changeLog[i].id}] in the change log")
        }

        if (changeLog[i].hash != change.hash) {
          throw new ChangeSetException("The hash [${toHex(change.hash)}] for $change does not match the hash " +
              "[${toHex(changeLog[i].hash)}] in the change log")
        }
      } else {
        session.execute(change.cql)
        session.execute(insertSchemaChange.bind((int) (i / bucketSize), i, change.id, new Date(), change.hash,
            change.author, change.description, change.tags))
      }
    }
  }

  Binding createBinding(List schemaChanges) {
    return new Binding(
      schemaChange: { schemaChanges << createSchemaChange(it) }
    )
  }

  def createSchemaChange(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = ChangeSet) Closure closure) {
    closure.delegate = new ChangeSet()
    def code = closure.rehydrate(closure.delegate, this, this)
    code.resolveStrategy = Closure.DELEGATE_FIRST
    code()
    return code.delegate
  }

  def createSchemaChangesTableIfNecessary() {
    def resultSet = session.execute(
        "SELECT * FROM system.schema_columnfamilies " +
        "WHERE keyspace_name = '$keyspace' AND columnfamily_name = '$CHANGELOG_TABLE'"
    )
    if (resultSet.exhausted) {
      session.execute("""
CREATE TABLE ${keyspace}.$CHANGELOG_TABLE(
  bucket int,
  revision int,
  id text,
  applied_at timestamp,
  hash blob,
  author text,
  description text,
  tags set<text>,
  PRIMARY KEY (bucket, revision)
)
""")
    }
  }

  def toHex(ByteBuffer buffer) {
    return buffer.array().encodeHex().toString()
  }

}
