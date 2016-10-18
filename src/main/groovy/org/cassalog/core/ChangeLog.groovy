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

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement

import static org.cassalog.core.CassalogImpl.CHANGELOG_TABLE

/**
 * This class provides a view of the change log stored in the database. Change log entries can be read after calling
 * the {@link ChangeLog#load() load} method. Creating and Updating the change log table is currently done by
 * SchemaManager.
 *
 * @author jsanda
 */
class ChangeLog {

  Session session

  /**
   * The keyspace in which the change log table lives.
   */
  String keyspace

  /**
   * Specifies the number of ChangeSets per bucket, i.e., the number of rows per partition in the change log table.
   */
  int bucketSize

  /**
   * The total number of revisions in the change log.
   */
  private int numRevisions

  /**
   * Each bucket corresponds to a partition in the change log table, and each bucket contains a list of ChangeSets.
   */
  def buckets = []

  ConsistencyLevel consistencyLevel

  private PreparedStatement loadBucket

  private PreparedStatement insertChangeSet

  private PreparedStatement updateAppliedAt

  void load() {
    initPreparedStatements()

    def findBuckets = new SimpleStatement("SELECT DISTINCT bucket FROM ${keyspace}.$CHANGELOG_TABLE")
    findBuckets.consistencyLevel = consistencyLevel
    def bucketResultSet = session.execute(findBuckets)

    if (!bucketResultSet.exhausted) {
      int bucket = bucketResultSet.all().max { it.getInt(0) }.getInt(0)
      def findRevisions = new SimpleStatement(
          "SELECT revision FROM ${keyspace}.$CHANGELOG_TABLE " +
          "WHERE bucket = $bucket " +
          "ORDER BY revision DESC"
      )
      findRevisions.consistencyLevel = consistencyLevel

      def revisionsResultSet = session.execute(findRevisions)
      numRevisions = revisionsResultSet.all().first().getInt(0) + 1
    }
  }

  private void initPreparedStatements() {
    loadBucket = session.prepare("""
      SELECT version, applied_at, hash, author, description, tags, writetime(hash)
      FROM ${keyspace}.$CHANGELOG_TABLE
      WHERE bucket = ?
      """
    )

    insertChangeSet = session.prepare("""
      INSERT INTO ${keyspace}.${CHANGELOG_TABLE} (bucket, revision, version, hash, author, description, tags,
applied_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      """
    )

    updateAppliedAt = session.prepare(
        "UPDATE ${keyspace}.$CHANGELOG_TABLE SET applied_at = ? WHERE bucket = ? AND revision = ?")
  }

  int getSize() {
    return numRevisions
  }

  def getAt(int revision) {
    int index = (int) (revision / bucketSize)
    if (index >= numRevisions) {
      return null
    }
    if (buckets[index] == null) {
      loadBucket(index)
    }
    def bucket = buckets[index]
    int offset = revision % bucketSize
    return bucket[offset]
  }

  def loadBucket(int bucket) {
    def resultSet = session.execute(loadBucket.bind(bucket))
    def changeSets = []
    resultSet.each { changeSets << toChangeSet(it) }
    buckets[bucket] = changeSets
  }

  def leftShift(ChangeSet changeSet) {
    add(changeSet)
  }

  def add(ChangeSet changeSet) {
    int index = (int) (numRevisions / bucketSize)
    if (buckets[index] == null) {
      buckets[index] = []
    }
    buckets[index] << changeSet
    session.execute(insertChangeSet.bind(index, numRevisions++, changeSet.version, changeSet.hash,
        changeSet.author, changeSet.description, changeSet.tags, changeSet.appliedAt))
  }

  def toChangeSet(Row row) {
    return new ChangeSet(
        version: row.getString(0),
        appliedAt: row.getTimestamp(1),
        hash: row.getBytes(2),
        author: row.getString(3),
        description: row.getString(4),
        tags: row.getSet(5, String),
        timestamp: new Date((row.getLong(6) / 1000) as Long)
    )
  }

}
