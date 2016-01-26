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
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.InvalidQueryException
import groovy.util.logging.Slf4j

import java.nio.ByteBuffer
import java.security.MessageDigest
/**
 * @author jsanda
 */
@Slf4j
class Cassalog {

  static final String CHANGELOG_TABLE = 'schema_changelog'

  static final int DEFAULT_BUCKET_SIZE = 50

  String keyspace

  Session session

  int bucketSize = DEFAULT_BUCKET_SIZE

  PreparedStatement insertSchemaChange

  void execute(URI script) {
    execute(script, Collections.emptySet())
  }

  void execute(URI script, Map vars) {
    execute(script, Collections.emptySet(), vars)
  }

  void execute(URI script, Collection tags) {
    execute(script, tags, [keyspace: keyspace])
  }

  void execute(URI script, Collection tags, Map vars) {
    log.info("Executing ${[script: script, tags: tags, vars: vars]}")

    def changeSets = []
    def changeLog

    GroovyShell shell = new GroovyShell(createBinding(changeSets, vars))
    shell.evaluate(script)

    if (changeSets[0] instanceof CreateKeyspace) {
      keyspace = changeSets[0].name
      if (changeSets[0].recreate) {
        log.debug("Dropping keyspace $keyspace")
        session.execute("DROP KEYSPACE IF EXISTS $keyspace")
      }
      if (!keyspaceExists()) {
        applyChangeSet(changeSets[0])
        createChangeLogTableIfNecessary()
        initPreparedStatements()

        session.execute(insertSchemaChange.bind(0, 0, changeSets[0].id, new Date(), changeSets[0].hash,
            changeSets[0].author, changeSets[0].description, changeSets[0].tags))
      }
    }

    if (keyspace == null) {
      throw new KeyspaceUndefinedException()
    }

    createChangeLogTableIfNecessary()

    changeLog = new ChangeLog(session: session, keyspace: keyspace, bucketSize: bucketSize)
    changeLog.load()

    initPreparedStatements()

    changeSets.eachWithIndex{ def change, int i ->
      change.validate()
      change.hash = computeHash(change.cql)

      if (change instanceof CreateKeyspace && change.active == true) {
        session.execute("USE $keyspace")
      }

      if (i < changeLog.size) {
        if (changeLog[i].id != change.id) {
          throw new ChangeSetAlteredException("The id [$change.id] for $change does not match the id " +
              "[${changeLog[i].id}] in the change log")
        }

        if (changeLog[i].hash != change.hash) {
          throw new ChangeSetAlteredException("The hash [${toHex(change.hash)}] for $change does not match the hash " +
              "[${toHex(changeLog[i].hash)}] in the change log")
        }
      } else {
        if (change.tags.empty || change.tags.containsAll(tags)) {
          try {
            applyChangeSet(change)
            session.execute(insertSchemaChange.bind((int) (i / bucketSize), i, change.id, new Date(), change.hash,
                change.author, change.description, change.tags))
          } catch (InvalidQueryException e) {
            throw new ChangeSetException(e)
          }
        }
      }
    }
  }

  Binding createBinding(List schemaChanges, Map vars) {
    Map scriptVars = new HashMap(vars)
    Binding binding = new Binding(scriptVars)
    scriptVars.createKeyspace = { schemaChanges << createKeyspace(it, binding) }
    scriptVars.schemaChange = { schemaChanges << createCqlChangeSet(it, binding) }

    def setKeyspace = { keyspace -> return new SetKeyspace(name: keyspace) }
    setKeyspace.delegate = binding

    scriptVars.setKeyspace = { schemaChanges << setKeyspace(it)}
    return new Binding(scriptVars)
  }

  def createKeyspace(Closure closure, Binding binding) {
    closure.delegate = new CreateKeyspace()
    def code = closure.rehydrate(closure.delegate, binding, this)
    code.resolveStrategy = Closure.DELEGATE_FIRST

    code()
    return code.delegate
  }

  def createCqlChangeSet(Closure closure, Binding binding) {
    closure.delegate = new CqlChangeSet()
    def code = closure.rehydrate(closure.delegate, binding, this)
    code.resolveStrategy = Closure.DELEGATE_FIRST
    code()
    return code.delegate
  }

  def include(String script) {

  }

  boolean keyspaceExists() {
    def resultSet = session.execute(
        "SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = '$keyspace'"
    )
    return !resultSet.exhausted
  }

  def initPreparedStatements() {
    if (insertSchemaChange != null) {
      return
    }
    insertSchemaChange = session.prepare("""
      INSERT INTO ${keyspace}.$CHANGELOG_TABLE (bucket, revision, id, applied_at, hash, author, description, tags)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      """
    )
  }

  def createChangeLogTableIfNecessary() {
    def resultSet = session.execute(
        "SELECT * FROM system.schema_columnfamilies " +
        "WHERE keyspace_name = '$keyspace' AND columnfamily_name = '$CHANGELOG_TABLE'"
    )
    if (resultSet.exhausted) {
      log.info("Creating change log table ${keyspace}.$CHANGELOG_TABLE")
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

  def applyChangeSet(ChangeSet changeSet) {
    log.info("""Applying ChangeSet
-- id: $changeSet.id
$changeSet.cql
--"""
    )
    session.execute(changeSet.cql)
  }

  def computeHash(String s) {
    def sha1 = MessageDigest.getInstance("SHA1")
    def digest = sha1.digest(s.bytes)
    return ByteBuffer.wrap(digest)
  }

  def toHex(ByteBuffer buffer) {
    return buffer.array().encodeHex().toString()
  }

}
