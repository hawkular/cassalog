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
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
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

  private PreparedStatement loadBucket

  void load() {
    loadBucket = session.prepare("""
      SELECT version, applied_at, hash, author, description, tags
      FROM ${keyspace}.$Cassalog.CHANGELOG_TABLE
      WHERE bucket = ?
      """
    )
    def bucketResultSet = session.execute("SELECT DISTINCT bucket FROM ${keyspace}.$Cassalog.CHANGELOG_TABLE")
    if (!bucketResultSet.exhausted) {
      int bucket = bucketResultSet.all().max { it.getInt(0) }.getInt(0)
      def revisionsResultSet = session.execute("""
        SELECT revision
        FROM ${keyspace}.$Cassalog.CHANGELOG_TABLE
        WHERE bucket = $bucket ORDER BY revision DESC
        """
      )
      numRevisions = revisionsResultSet.all().first().getInt(0) + 1
    }
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
      initBucket(index)
    }
    def bucket = buckets[index]
    int offset = revision % bucketSize
    return bucket[offset]
  }

  def initBucket(int bucket) {
    def resultSet = session.execute(loadBucket.bind(bucket))
    def changeSets = []
    resultSet.each { changeSets << toChangeSet(it) }
    buckets[bucket] = changeSets
  }

  def toChangeSet(Row row) {
    return new ChangeSet(
        version: row.getString(0),
        appliedAt: row.getTimestamp(1),
        hash: row.getBytes(2),
        author: row.getString(3),
        description: row.getString(4),
        tags: row.getSet(5, String)
    )
  }

}
