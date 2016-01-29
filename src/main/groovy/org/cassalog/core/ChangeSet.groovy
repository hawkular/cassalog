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

import java.nio.ByteBuffer
import java.security.MessageDigest

/**
 * Specifies schema changes to be applied. The changes are in form of CQL statements along with an id and some optional
 * meta data.
 *
 * @author jsanda
 */
@ToString(includeNames = true)
@EqualsAndHashCode(includeFields = true)
class ChangeSet {

  /**
   * A user-defined version for the ChangeSet. Version strings should be unique; however, Cassalog does not enforce
   * it. It should also be noted that Cassalog does not enforce ordering of changes based on the version.
   */
  String version

  /**
   * A timestamp of when the schema changes are made.
   */
  Date appliedAt

  /**
   * A hash of the CQL that gets stored in the database. It is used to detect whether or not the change set has been
   * altered since the schema changes were applied.
   */
  ByteBuffer hash

  /**
   * A user-defined, optional set of tags. This field is persisted but currently not used. In the future there will be
   * support for executing change sets based on some tag matching criteria.
   */
  Set tags = [] as Set

  /**
   * An optional field for specifying who made the change.
   */
  String author

  /**
   * An optional field for providing a description or some comments about the change.
   */
  String description

  ByteBuffer getHash() {
    if (hash == null) {
      def sha1 = MessageDigest.getInstance("SHA1")
      def digest = sha1.digest(cql.bytes)
      hash = ByteBuffer.wrap(digest)
    }
    return hash
  }

  void version(String version) {
    this.version = version
  }

  void author(String author) {
    this.author = author
  }

  void description(String description) {
    this.description = description;
  }

  void tags(String... tags) {
    this.tags = tags as Set
  }

  /**
   * Perform basic validation to ensure required fields are set. Subclasses should override this to perform
   * additional validation.
   *
   * @throws ChangeSetValidationException if {@link #getVersion()} is null
   */
  void validate() {
    if (version == null) {
      throw new ChangeSetValidationException('The version property must be set')
    }
  }

}
