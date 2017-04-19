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

import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.exceptions.InvalidQueryException
import groovy.util.logging.Slf4j

import java.nio.ByteBuffer
/**
 * @author jsanda
 */
@Slf4j
class CassalogImpl implements Cassalog {

  static final String CHANGELOG_TABLE = 'cassalog'

  static final int DEFAULT_BUCKET_SIZE = 50

  String keyspace

  Session session

  String baseScriptsPath

  int bucketSize = DEFAULT_BUCKET_SIZE

  PreparedStatement insertSchemaChange

  PreparedStatement updateAppliedAt

  ConsistencyLevel consistencyLevel

  @Override
  void execute(URI script) {
    execute(script, Collections.emptySet())
  }

  @Override
  void execute(URI script, Map vars) {
    execute(script, Collections.emptySet(), vars)
  }

  @Override
  void execute(URI script, Collection tags) {
    execute(script, tags, [keyspace: keyspace])
  }

  @Override
  void execute(URI script, Collection tags, Map vars) {
    verifyCassandraVersion()
    verifySchemaAgreement()
    determineConsistencyLevel()

    log.info("Executing ${[script: script, tags: tags, vars: vars]}")

    def changeSets = []
    def changeLog

    GroovyShell shell = new GroovyShell(createBinding(changeSets, vars))
    shell.evaluate(script)

    if (changeSets[0] instanceof CreateKeyspace) {
      keyspace = changeSets[0].name
      if (changeSets[0].recreate) {
        log.debug("Dropping keyspace $keyspace")
        executeCQL("DROP KEYSPACE IF EXISTS $keyspace")
      }
      if (!keyspaceExists()) {
        applyChangeSet(changeSets[0], 0, false)
        createChangeLogTableIfNecessary()
        initPreparedStatements()
        executeCQL(insertSchemaChange.bind(0, 0, changeSets[0].version, changeSets[0].hash, changeSets[0].author,
            changeSets[0].description, changeSets[0].tags))
        executeCQL(updateAppliedAt.bind(new Date(), 0, 0))
      }
    }

    if (keyspace == null) {
      throw new KeyspaceUndefinedException()
    }

    createChangeLogTableIfNecessary()

    changeLog = new ChangeLog(session: session, keyspace: keyspace, bucketSize: bucketSize,
        consistencyLevel: consistencyLevel)
    changeLog.load()

    initPreparedStatements()

    def tagsFilter = { changeSetTags -> tags.empty || changeSetTags.any { tags.contains(it) } }

    changeSets.findAll {change -> change.tags.empty || tagsFilter(change.tags)}.eachWithIndex { def change, int i ->
      change.validate()

      if (change instanceof CreateKeyspace && change.active == true) {
        executeCQL("USE $keyspace")
      }

      if (change.alwaysRun) {
        if (i < changeLog.size) {
          validateChange(change, changeLog[i])
          applyChangeSet(change, i, false)
        } else {
          applyChangeSet(change, i, true)
        }

      } else if (i < changeLog.size) {
        validateChange(change, changeLog[i])
        if (changeLog[i].appliedAt == null) {
          if (change.verifyFunction != null && change.verifyFunction()) {
            updateAppliedAt(i, changeLog[i].timestamp)
          } else {
            applyChangeSet(change, i, true)
          }
        }
      } else {
        try {
          applyChangeSet(change, i, true)
        } catch (InvalidQueryException e) {
          throw new ChangeSetException(e)
        }
      }
    }
  }

  private void validateChange(change, appliedChange) {
    if (appliedChange.version != change.version) {
      throw new ChangeSetAlteredException("The version [$change.version] for $change does not match the version " +
          "[${appliedChange.version}] in the change log")
    }

    if (appliedChange.hash != change.hash) {
      throw new ChangeSetAlteredException("The hash [${toHex(change.hash)}] for $change does not match the hash " +
          "[${toHex(appliedChange.hash)}] in the change log")
    }
  }

  Binding createBinding(List schemaChanges, Map vars) {
    Map scriptVars = new HashMap(vars)
    Binding binding = new Binding(scriptVars)

    scriptVars.createKeyspace = { schemaChanges << createKeyspace(it, binding) }
    scriptVars.schemaChange = { schemaChanges << createCqlChangeSet(it, binding) }
    scriptVars.setKeyspace = { schemaChanges << new SetKeyspace(name: keyspace) }
    scriptVars.include = { schemaChanges.addAll(include(it, vars)) }
    scriptVars.putAll(createScriptHelperFunctions())


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
    def code = closure
    code.resolveStrategy = Closure.DELEGATE_FIRST
    code()
    return code.delegate
  }

  def include(String script, Map vars) {
    def dbchanges = []

    def scriptVars = new HashMap(vars)
    def binding = new Binding(scriptVars)

    scriptVars.createKeyspace = { dbchanges << createKeyspace(it, binding) }
    scriptVars.schemaChange = { dbchanges << createCqlChangeSet(it, binding) }
    scriptVars.setKeyspace = { dbchanges << new SetKeyspace(name: keyspace) }
    scriptVars.include = { dbchanges.addAll(include(it, vars)) }
    scriptVars.putAll(createScriptHelperFunctions())

    def shell = new GroovyShell(binding)
    def scriptURI
    if (baseScriptsPath != null && baseScriptsPath != "") {
      scriptURI = new File(baseScriptsPath + script).toURI()
    } else {
      scriptURI = getClass().getResource(script).toURI()
    }
    shell.evaluate(scriptURI)

    return dbchanges
  }

  Map createScriptHelperFunctions() {
    def verificationFunctions = new VerificationFunctions(session: session)
    return [
        keyspaceExists: { return keyspaceExists(it) },
        isSchemaVersioned: { return isSchemaVersioned(it) },
        getTables: {
          def resultSet
          def tables = []
          if (cassandraMajorVersion == 2) {
            resultSet = executeCQL("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE " +
                "keyspace_name = '$it'")
          } else {
            resultSet = executeCQL("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '$it'")
          }
          resultSet.all().each { tables << it.getString(0) }
          return tables
        },
        getUDTs: {
          def resultSet
          def types = []
          if (cassandraMajorVersion == 2) {
            resultSet = executeCQL("SELECT type_name FROM system.schema_usertypes WHERE keyspace_name = '$it'")
          } else {
            resultSet = executeCQL("SELECT type_name FROM system_schema.types WHERE keyspace_name = '$it'")
          }
          resultSet.all().each { types << it.getString(0) }
          return types
        },
        tableExists: verificationFunctions.&tableExists,
        tableDoesNotExist: verificationFunctions.&tableDoesNotExist,
        columnExists: verificationFunctions.&columnExists,
        columnDoesNotExist: verificationFunctions.&columnDoesNotExist,
        typeExists: verificationFunctions.&typeExists,
        typeDoesNotExist: verificationFunctions.&typeDoesNotExist
    ]
  }

  boolean keyspaceExists() {
    return keyspaceExists(keyspace)
  }

  boolean keyspaceExists(String keyspace) {
    def resultSet
    if (cassandraMajorVersion == 2) {
      resultSet = executeCQL(
          "SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = '$keyspace'"
      )
    } else {
      resultSet = executeCQL(
          "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '$keyspace'"
      )
    }
    return !resultSet.exhausted
  }

  def initPreparedStatements() {
    if (insertSchemaChange != null) {
      return
    }
    insertSchemaChange = session.prepare("""
      INSERT INTO ${keyspace}.$CHANGELOG_TABLE (bucket, revision, version, hash, author, description, tags)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      """
    )

    updateAppliedAt = session.prepare(
        "UPDATE ${keyspace}.$CHANGELOG_TABLE SET applied_at = ? WHERE bucket = ? AND revision = ?")
  }

  def createChangeLogTableIfNecessary() {
    if (!isSchemaVersioned()) {
      log.info("Creating change log table ${keyspace}.$CHANGELOG_TABLE")
      executeCQL("""
CREATE TABLE ${keyspace}.$CHANGELOG_TABLE(
  bucket int,
  revision int,
  version text,
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

  boolean isSchemaVersioned() {
    return isSchemaVersioned(keyspace)
  }

  boolean isSchemaVersioned(String keyspace) {
    def resultSet
    if (cassandraMajorVersion == 2) {
      resultSet = executeCQL(
          "SELECT * FROM system.schema_columnfamilies " +
          "WHERE keyspace_name = '$keyspace' AND columnfamily_name = '$CHANGELOG_TABLE'"
      )
    } else {
      resultSet = executeCQL(
          "SELECT * FROM system_schema.tables " +
          "WHERE keyspace_name = '$keyspace' AND table_name = '$CHANGELOG_TABLE'"
      )
    }

    return !resultSet.exhausted
  }

  def applyChangeSet(ChangeSet changeSet, int index, boolean updateChangeLog) {
    log.info("""Applying ChangeSet
-- version: $changeSet.version
${changeSet.cql.join('\n')}
--"""
    )
    if (updateChangeLog) {
      insertChangeSet(changeSet, index)
    }
    changeSet.cql.each { executeCQL(it) }
    if (updateChangeLog) {
      updateAppliedAt(index)
    }
  }

  def insertChangeSet(ChangeSet changeSet, int index) {
    int bucket = index / bucketSize
    executeCQL(insertSchemaChange.bind(bucket, index, changeSet.version, changeSet.hash, changeSet.author,
        changeSet.description, changeSet.tags))
  }

  def updateAppliedAt(int index) {
    updateAppliedAt(index, new Date())
  }

  def updateAppliedAt(int index, Date timestamp) {
    int bucket = index / bucketSize
    executeCQL(updateAppliedAt.bind(timestamp, bucket, index))
  }

  def toHex(ByteBuffer buffer) {
    return buffer.array().encodeHex().toString()
  }

  def executeCQL(String cql) {
    def statement = new SimpleStatement(cql)
    statement.consistencyLevel = consistencyLevel
    return session.execute(statement)
  }

  def executeCQL(BoundStatement statement) {
    try {
      statement.consistencyLevel = consistencyLevel
      return session.execute(statement)
    } catch (Exception e) {
      log.error("CQL error", e)
      return null
    }
  }

  def determineConsistencyLevel() {
    def resultSet = session.execute("SELECT peer FROM system.peers")
    int clusterSize = resultSet.all().size()
    if (clusterSize == 0) {
      consistencyLevel = ConsistencyLevel.LOCAL_ONE
    } else {
      consistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    }
  }

  /**
   * Verifies that all nodes are running the same major version and that the major version is at least 2.
   */
  def verifyCassandraVersion() {
    def majorVersion = null
    session.cluster.metadata.allHosts.each { host ->
      if (majorVersion == null) {
        // TODO make this null safe
        majorVersion = host.cassandraVersion.major
      } else {
        if (host.cassandraVersion != null && host.cassandraVersion.major != majorVersion) {
          throw new CassandraVersionException("All Cassandra nodes should be running the same major version.")
        }
      }
    }
    if (majorVersion == 1) {
      throw new CassandraVersionException("Cassandra 1.x is not supported.")
    }
  }

  int getCassandraMajorVersion() {
    return session.cluster.metadata.allHosts.find { host -> host.cassandraVersion != null}.cassandraVersion.major
  }

  def verifySchemaAgreement() {
    if (!session.cluster.metadata.checkSchemaAgreement()) {
      throw new SchemaAgreementException()
    }
  }

}
