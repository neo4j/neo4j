/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.ShortestPath
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{ListValue, PathValue, VirtualValues}

import scala.collection.JavaConverters._

/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 */
case class ShortestPathPipe(source: Pipe, shortestPathCommand: ShortestPath, predicates: Seq[Predicate] = Seq.empty,
                            withFallBack: Boolean = false, disallowSameNode: Boolean = true)
                           (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(source) {
  private def pathName = shortestPathCommand.pathName
  private val shortestPathExpression = ShortestPathExpression(shortestPathCommand, predicates, withFallBack, disallowSameNode)

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
            case(path: PathValue) => ctx.newWith2(pathName, path, relName, VirtualValues.list(path.edges():_*))
            case _ => throw new InternalException("We expect only paths here")
          }
        case None =>
          result.iterator().asScala.map {
            case path: PathValue => ctx.newWith1(pathName, path)
            case _ => throw new InternalException("We expect only paths here")
          }
      }
    })
}
