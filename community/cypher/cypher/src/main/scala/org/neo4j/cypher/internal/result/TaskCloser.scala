/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.result

import org.neo4j.internal.helpers.Exceptions
import org.neo4j.kernel.api.exceptions.Status

import scala.collection.mutable.ArrayBuffer

sealed trait CloseReason
case object Success extends CloseReason

case object Failure extends CloseReason {
  val status = Status.Transaction.QueryExecutionFailedOnTransaction
}
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
              case Some(first) => Exceptions.chain(first, e)
              case None        => foundException = Some(e)
            }
        }
      }

      foundException.forall(throwable => throw throwable)
    }
  }

  def isClosed: Boolean = closed
}
