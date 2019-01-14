/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import scala.collection.mutable.ArrayBuffer

class TaskCloser {

  private val _tasks: ArrayBuffer[Boolean => Unit] = ArrayBuffer.empty
  private var closed = false

  /**
   *
   * @param task This task will be called, with true if the query went OK, and a false if an error occurred
   */
  def addTask(task: Boolean => Unit) {
    if(closed)
      throw new IllegalStateException("Already closed")
    _tasks += task
  }

  def close(success: Boolean) {
    if (!closed) {
      closed = true
      var foundException: Option[Throwable] = None
      _tasks.reverse foreach {
        f =>
          try {
            f(success)
          } catch {
            case e: Throwable =>
              foundException match {
                case Some(first) => first.addSuppressed(e)
                case None => foundException = Some(e)
              }
          }
      }

      foundException.forall(throwable => throw throwable)
    }
  }

  def isClosed: Boolean = closed
}
