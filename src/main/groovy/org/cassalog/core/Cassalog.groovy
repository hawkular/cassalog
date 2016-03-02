package org.cassalog.core

/**
 * @author jsanda
 */
interface Cassalog {

  void execute(URI script)

  void execute(URI script, Map vars)

  void execute(URI script, Collection tags)

  void execute(URI script, Collection tags, Map vars)
}