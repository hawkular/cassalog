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

import com.datastax.driver.core.Session

/**
 * @author jsanda
 */
class VerificationFunctions {

  Session session

  def getKeyspaceMetaData(String keyspace) {
    return session.cluster.metadata.keyspaces.find { it.name == keyspace }
  }

  boolean tableExists(String keyspace, String table) {
    return getKeyspaceMetaData(keyspace)?.tables?.find { it.name == table } != null
  }

  boolean tableDoesNotExist(String keyspace, String table) {
    return !tableExists(keyspace, table)
  }

  boolean columnExists(String keyspace, String table, String column) {
    def tableMetaData = getKeyspaceMetaData(keyspace)?.tables?.find { it.name == table }
    return tableMetaData?.columns?.find { it.name == column } != null
  }

  boolean columnDoesNotExist(String keyspace, String table, String column) {
    return !columnExists(keyspace, table, column)
  }

  boolean typeExists(String keyspace, String type) {
    def typesMetaData = getKeyspaceMetaData(keyspace)?.userTypes
    return typesMetaData?.find { it.name.name() == type } != null
  }

  boolean typeDoesNotExist(String keyspace, String type) {
    return !typeExists(keyspace, type)
  }

}
