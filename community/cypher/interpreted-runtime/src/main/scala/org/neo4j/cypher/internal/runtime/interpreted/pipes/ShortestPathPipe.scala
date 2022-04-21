/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.ScalaSeqAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualPathValue
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 */
case class ShortestPathPipe(source: Pipe, shortestPathExpression: ShortestPathExpression)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  private val shortestPathCommand = shortestPathExpression.shortestPathPattern
  private def pathName = shortestPathCommand.pathName

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    input.flatMap(ctx => {
      val result = shortestPathExpression(ctx, state, memoryTracker) match {
        case in: ListValue             => in
        case v if v eq Values.NO_VALUE => VirtualValues.EMPTY_LIST
        case path: VirtualPathValue    => VirtualValues.list(path)
      }

      val iterator = shortestPathCommand.relIterator match {
        case Some(relName) =>
          result.iterator().asScala.map {
            case path: VirtualPathValue =>
              val relations = VirtualValues.list(path.relationshipIds().map(VirtualValues.relationship): _*)
              rowFactory.copyWith(ctx, pathName, path, relName, relations)

            case value =>
              throw new InternalException(s"Expected path, got '$value'")
          }
        case None =>
          result.iterator().asScala.map {
            case path: VirtualPathValue =>
              rowFactory.copyWith(ctx, pathName, path)

            case value =>
              throw new InternalException(s"Expected path, got '$value'")
          }
      }
      iterator.asClosingIterator
    })
  }
}
