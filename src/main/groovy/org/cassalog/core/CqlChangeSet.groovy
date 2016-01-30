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
class CqlChangeSet extends ChangeSet {

  /**
   * The CQL to execute. It is not stored in the database.
   */
//  String cql
  def cql = []

  void cql(String cql) {
    this.cql << cql;
  }

  void cql(List cql) {
    println "CQL: $cql"
    this.cql.addAll(cql)
  }

  void validate() {
    super.validate()
//    if (cql == null) {
    if (cql.empty) {
      throw new ChangeSetValidationException('The cql property must be set')
    }
  }

}
