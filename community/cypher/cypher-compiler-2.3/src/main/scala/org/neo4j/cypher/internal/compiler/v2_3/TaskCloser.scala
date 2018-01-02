/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3

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
      val errors = _tasks.toSeq.flatMap {
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
