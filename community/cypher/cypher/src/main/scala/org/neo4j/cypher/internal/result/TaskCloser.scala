/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.result

import scala.collection.mutable.ArrayBuffer

sealed trait CloseReason
case object Success extends CloseReason
case object Failure extends CloseReason
case class Error(t: Throwable) extends CloseReason

class TaskCloser {

  private val _tasks: ArrayBuffer[CloseReason => Unit] = ArrayBuffer.empty
  private var closed = false

  /**
   *
   * @param task This task will be called, with true if the query went OK, and a false if an error occurred
   */
  def addTask(task: CloseReason => Unit): Unit = {
    if (closed)
      throw new IllegalStateException("Already closed")
    _tasks += task
  }

  def close(closeReason: CloseReason): Unit = {
    if (!closed) {
      closed = true
      var foundException: Option[Throwable] = None
      val iterator = _tasks.reverseIterator
      while (iterator.hasNext) {
        val f = iterator.next()
        try {
          f(closeReason)
        } catch {
          case e: Throwable =>
            foundException match {
              case Some(first) => first.addSuppressed(e)
              case None        => foundException = Some(e)
            }
        }
      }

      foundException.forall(throwable => throw throwable)
    }
  }

  def isClosed: Boolean = closed
}
