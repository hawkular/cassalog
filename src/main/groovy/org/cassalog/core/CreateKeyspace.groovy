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

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * @author jsanda
 */
@ToString(includeNames = true, includeSuper = true)
@EqualsAndHashCode(includeFields = true, callSuper = true)
class CreateKeyspace extends ChangeSet {

  /**
   * The keyspace name
   */
  String name

  /**
   * If set to true, Cassalog will set this as the active keyspace when applying changes. True by default.
   */
  boolean active = true

  boolean durableWrites

  /**
   * If set to true, Cassalog will drop the keyspace if it exists before creating it. This behavior only applies to
   * the initial change set. This setting can be useful in an environment for running automated tests in which you
   * want to start with a new schema for each test run. False by default.
   */
  boolean recreate

  ReplicationSettings replication = new ReplicationSettings()

  List getCql() {
    def cql = """
CREATE KEYSPACE $name WITH replication = { 'class': '${replication.strategy}', 'replication_factor': ${replication.replicationFactor} }
"""
    return [cql]
  }

  void name(String name) {
    this.name = name
  }

  void active(boolean use) {
    this.active = use
  }

  void recreate(boolean recreate) {
    this.recreate = recreate
  }

  void durable_writes(durableWrites) {
    this.durableWrites = durableWrites
  }

  void replication(Closure closure) {
    closure.delegate = replication
    closure.resolveStrategy = Closure.DELEGATE_ONLY
    closure()
  }

  void validate() {
    super.validate()
    if (name == null) {
      throw new ChangeSetValidationException('The name property must be set')
    }
  }

}
