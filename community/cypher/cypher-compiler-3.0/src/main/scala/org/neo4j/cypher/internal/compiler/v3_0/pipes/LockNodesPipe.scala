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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.graphdb.Node

case class LockNodesPipe(src: Pipe, variablesToLock: Set[String])(val estimatedCardinality: Option[Double] = None)
                        (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(src, pipeMonitor) with NoEffectsPipe with RonjaPipe {
  def symbols: SymbolTable = src.symbols

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map { ctx =>
      val nodesToLock: Set[Long] = variablesToLock.flatMap { varName =>
        Option(ctx(varName).asInstanceOf[Node]).map(_.getId)
      }
      state.query.lockNodes(nodesToLock.toSeq: _*)
      ctx
    }

  override def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "Lock", variables, KeyNames(variablesToLock.toSeq))

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (src :: Nil) = sources
    copy(src = src)(estimatedCardinality)
  }
}
