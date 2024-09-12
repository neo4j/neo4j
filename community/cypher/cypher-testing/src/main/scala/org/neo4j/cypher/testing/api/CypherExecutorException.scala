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

import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.GqlRuntimeException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * Exception abstraction for CypherExecutor:s
 * All exceptions thrown when executing cypher in a CypherExecutor should be converted into CypherExecutorException:s
 */
case class CypherExecutorException(
  errorGqlStatusObject: ErrorGqlStatusObject,
  override val status: Status,
  original: Throwable,
  message: Option[String]
) extends GqlRuntimeException(errorGqlStatusObject, message.getOrElse(original.getMessage))
    with HasStatus {

  def this(errorGqlStatusObject: ErrorGqlStatusObject, status: Status, original: Throwable) =
    this(errorGqlStatusObject, status, original, None)
  def this(status: Status, original: Throwable, message: Option[String]) = this(null, status, original, message)
  def this(status: Status, original: Throwable) = this(null, status, original, None)

  override def legacyMessage: String = if (message.isDefined) message.get
  else {
    original match {
      case e: ErrorGqlStatusObject => e.legacyMessage
      case _                       => original.getMessage
    }
  }
}

object CypherExecutorException {

  def unapply(cypherExecutorException: CypherExecutorException): Option[(Status, Throwable, Option[String])] = {
    Some((cypherExecutorException.status, cypherExecutorException.original, cypherExecutorException.message))
  }

  def apply(gqlStatusObject: ErrorGqlStatusObject, status: Status, original: Throwable): CypherExecutorException = {
    new CypherExecutorException(gqlStatusObject, status, original, None)
  }

  def apply(status: Status, original: Throwable, message: Option[String]): CypherExecutorException = {
    new CypherExecutorException(null, status, original, message)
  }

  def apply(status: Status, original: Throwable): CypherExecutorException = {
    new CypherExecutorException(null, status, original, None)
  }

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
