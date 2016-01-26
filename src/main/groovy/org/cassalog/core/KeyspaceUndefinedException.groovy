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

/**
 * Thrown when trying to run Cassalog without having set the keyspace. There are two ways in which the keyspace can be
 * set. First is by setting the keyspace property on the Cassalog instance. Second is by using the createKeyspace
 * function as the first function in a script. When the createKeyspace function is the first function used in a script,
 * Cassalog will used the specified keyspace for the script execution. That is, it will look for or create the change
 * log table in that script.
 *
 * @author jsanda
 */
class KeyspaceUndefinedException extends RuntimeException {

  KeyspaceUndefinedException() {
    super()
  }

  KeyspaceUndefinedException(String message) {
    super(message)
  }
}
