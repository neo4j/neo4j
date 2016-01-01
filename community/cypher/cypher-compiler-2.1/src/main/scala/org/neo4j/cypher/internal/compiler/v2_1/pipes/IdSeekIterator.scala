/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.graphdb.PropertyContainer
import org.neo4j.cypher.internal.compiler.v2_1.spi.Operations
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.NumericHelper
import org.neo4j.cypher.EntityNotFoundException

class IdSeekIterator[T <: PropertyContainer](ident: String, operations: Operations[T], nodeIds: Iterator[Any])
  extends Iterator[ExecutionContext] with NumericHelper {

  private var cached = cacheNext()

  def hasNext = cached.isDefined

  def next() = cached match {
    case Some(result) =>
      cached = cacheNext()
      ExecutionContext.from(ident -> result)
    case None =>
      Iterator.empty.next
  }

  private def cacheNext(): Option[T] = {
    if (nodeIds.hasNext) {
      val id = asLongEntityId(nodeIds.next())
      try {
        Some(operations.getById(id))
      } catch {
        case _: EntityNotFoundException => cacheNext()
      }
    } else {
      None
    }
  }
}
