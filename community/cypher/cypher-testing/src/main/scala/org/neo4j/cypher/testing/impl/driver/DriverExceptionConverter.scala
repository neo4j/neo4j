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
package org.neo4j.cypher.testing.impl.driver

import org.neo4j.cypher.testing.api.CypherExecutorException
import org.neo4j.cypher.testing.api.CypherExecutorException.ExceptionConverter
import org.neo4j.driver.exceptions.Neo4jException

import scala.annotation.tailrec

trait DriverExceptionConverter extends ExceptionConverter {

  def asExecutorException(throwable: Throwable): Option[CypherExecutorException] = for {
    exception <- findDriverException(throwable)
    status <- statusOf(exception.code())
    message = exception.getMessage
  } yield CypherExecutorException(status, throwable, Some(message))

  @tailrec
  private def findDriverException(throwable: Throwable): Option[Neo4jException] = throwable match {
    case null                  => None
    case found: Neo4jException => Some(found)
    case _                     => findDriverException(throwable.getCause)
  }
}
