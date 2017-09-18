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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

import scala.collection.mutable.ListBuffer

case class OptionalExpandIntoPipe(source: Pipe, fromName: String, relName: String, toName: String,
                                  dir: SemanticDirection, types: LazyTypes, predicate: Predicate)
                                 (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(source) with CachingExpandInto {
  private final val CACHE_SIZE = 100000

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new RelationshipsCache(CACHE_SIZE)

    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        fromNode match {
          case fromNode: NodeValue =>
            val toNode = getRowNode(row, toName)

            toNode match {
              case Values.NO_VALUE => Iterator.single(row.newWith1(relName, Values.NO_VALUE))
              case n: NodeValue =>
                val relationships = relCache.get(fromNode, n, dir)
                  .getOrElse(findRelationships(state.query, fromNode, n, relCache, dir, types.types(state.query)))

                val it = relationships.toIterator
                val filteredRows = ListBuffer.empty[ExecutionContext]
                while (it.hasNext) {
                  val candidateRow = row.newWith1(relName, it.next())
                  if (predicate.isTrue(candidateRow, state)) {
                    filteredRows.append(candidateRow)
                  }
                }

                if (filteredRows.isEmpty) Iterator.single(row.newWith1(relName, Values.NO_VALUE))
                else filteredRows
            }

          case Values.NO_VALUE => Iterator(row.newWith1(relName, Values.NO_VALUE))
        }
    }
  }
}
