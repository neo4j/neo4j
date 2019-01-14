/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.ShortestPath
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{ListValue, PathValue, VirtualValues}

import scala.collection.JavaConverters._

/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 */
case class ShortestPathPipe(source: Pipe, shortestPathExpression: ShortestPathExpression,
                            withFallBack: Boolean = false, disallowSameNode: Boolean = true)
                           (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {
  private val shortestPathCommand = shortestPathExpression.shortestPathPattern
  private def pathName = shortestPathCommand.pathName

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState) =
    input.flatMap(ctx => {
      val result = shortestPathExpression(ctx, state) match {
        case in: ListValue => in
        case v if v == Values.NO_VALUE => VirtualValues.EMPTY_LIST
        case path: PathValue    => VirtualValues.list(path)
      }

      shortestPathCommand.relIterator match {
        case Some(relName) =>
          result.iterator().asScala.map {
            case path: PathValue =>
              val relations = VirtualValues.list(path.relationships(): _*)
              executionContextFactory.copyWith(ctx, pathName, path, relName, relations)

            case value =>
              throw new InternalException(s"Expected path, got '$value'")
          }
        case None =>
          result.iterator().asScala.map {
            case path: PathValue =>
              executionContextFactory.copyWith(ctx, pathName, path)

            case value =>
              throw new InternalException(s"Expected path, got '$value'")
          }
      }
    })
}
