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
package org.neo4j.cypher.testing.api

import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * Exception abstraction for CypherExecutor:s
 * All exceptions thrown when executing cypher in a CypherExecutor should be converted into CypherExecutorException:s
 */
case class CypherExecutorException(
  status: Status,
  original: Throwable,
  message: Option[String] = None
) extends RuntimeException(message.getOrElse(original.getMessage)) with HasStatus

object CypherExecutorException {

  trait ExceptionConverter {

    def convertExceptions[T](code: => T): T =
      try {
        code
      } catch {
        case t: Throwable => throw asExecutorException(t).getOrElse(t)
      }

    def statusOf(code: String): Option[Status] =
      Status.Code.all.asScala.find(status => status.code.serialize == code)

    def asExecutorException(throwable: Throwable): Option[CypherExecutorException]
  }
}
