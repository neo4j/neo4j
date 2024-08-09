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
package org.neo4j.cypher.internal.planning

import org.neo4j.collection.ResourceRawIterator
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException

final class ExceptionWrappingProcedureIterator[T, E <: Exception](
  private[this] val inner: ResourceRawIterator[T, E]
) extends ResourceRawIterator[T, E] {

  override def hasNext: Boolean =
    try {
      inner.hasNext
    } catch {
      // Procedures always wraps exceptions in a ProcedureException
      case e: ProcedureException => throw new CypherExecutionException(e.getMessage, e)
    }

  override def next(): T =
    try {
      inner.next()
    } catch {
      // Procedures always wraps exceptions in a ProcedureExceptions
      case e: ProcedureException => throw new CypherExecutionException(e.getMessage, e)
    }

  override def close(): Unit =
    try {
      inner.close()
    } catch {
      // Procedures always wraps close exceptions in a ResourceCloseFailureException
      case e: ResourceCloseFailureException => throw new CypherExecutionException(e.getMessage, e)
    }
}
