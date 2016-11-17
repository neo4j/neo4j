/*
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{Effects, ReadsAllNodes, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.graphdb.Node

import scala.collection.mutable.ListBuffer

case class OptionalExpandIntoPipe(source: Pipe, fromName: String, relName: String, toName: String,
                                  dir: SemanticDirection, types: LazyTypes, predicate: Predicate)
                                (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe with CachingExpandInto {
  private final val CACHE_SIZE = 100000

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new RelationshipsCache(CACHE_SIZE)

    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        fromNode match {
          case fromNode: Node =>
            val toNode = getRowNode(row, toName)

            if (toNode == null) Iterator.single(row.newWith1(relName, null))
            else {
              val relationships = relCache.get(fromNode, toNode, dir)
                .getOrElse(findRelationships(state.query, fromNode, toNode, relCache, dir, types.types(state.query)))

              val it = relationships.toIterator
              val filteredRows = ListBuffer.empty[ExecutionContext]
              while (it.hasNext) {
                val candidateRow = row.newWith1(relName, it.next())

                if (predicate.isTrue(candidateRow)(state)) {
                  filteredRows.append(candidateRow)
                }
              }

              if (filteredRows.isEmpty) Iterator.single(row.newWith1(relName, null))
              else filteredRows
            }

          case null => Iterator(row.newWith1(relName, null))
        }
    }
  }

  def planDescriptionWithoutCardinality = {
    val expandExpr = ExpandExpression(fromName, relName, types.names, toName, dir, minLength = 1, maxLength = Some(1))
    source.planDescription.
      andThen(this.id, "OptionalExpand(Into)", variables, expandExpr)
  }

  def symbols = source.symbols.add(toName, CTNode).add(relName, CTRelationship)

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  override def localEffects = predicate.effects(symbols) ++ Effects(ReadsAllNodes, ReadsAllRelationships)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
