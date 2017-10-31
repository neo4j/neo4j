/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.v3_4

import scala.collection.mutable.ListBuffer

class TaskCloser {

  private val _tasks: ListBuffer[Boolean => Unit] = ListBuffer.empty
  private var closed = false

  /**
   *
   * @param task This task will be called, with true if the query went OK, and a false if an error occurred
   */
  def addTask(task: Boolean => Unit) {
    _tasks += task
  }

  def close(success: Boolean) {
    if (!closed) {
      closed = true
      val errors = _tasks.flatMap {
        f =>
          try {
            f(success)
            None
          } catch {
            case e: Throwable => Some(e)
          }
      }

      errors.map(e => throw e)
    }
  }

  def isClosed = closed
}
