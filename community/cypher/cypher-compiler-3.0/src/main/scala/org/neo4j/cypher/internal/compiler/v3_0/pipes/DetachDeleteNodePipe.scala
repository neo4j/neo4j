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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{DeletesNode, Effects}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v3_0.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.frontend.v3_0.{SemanticDirection, CypherTypeException}
import org.neo4j.graphdb.Node

case class DetachDeleteNodePipe(src: Pipe, expression: Expression)(val estimatedCardinality: Option[Double] = None)
                           (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(src, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions with CollectionSupport {

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      expression(row)(state) match {
        case n: Node =>
          if (!state.query.nodeOps.isDeleted(n)) {
            val rels = state.query.getRelationshipsForIds(n, SemanticDirection.BOTH, None)
            rels.foreach(state.query.relationshipOps.delete)
            state.query.nodeOps.delete(n)
          }
          row
        case null => row
        case other => throw new
            CypherTypeException(s"Expected to delete a Node but got a ${other.getClass.getSimpleName}")
      }
    }
  }

  def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "DetachDeleteNode", variables)

  def symbols = src.symbols

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    DeleteNodePipe(onlySource, expression)(estimatedCardinality)
  }

  override def localEffects = Effects(DeletesNode)
}
