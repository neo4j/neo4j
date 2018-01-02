/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable

case class SemiApplyPipe(source: Pipe, inner: Pipe, negated: Boolean)
                        (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {
  def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.filter {
      (outerContext) =>
        val innerState = state.withInitialContext(outerContext)
        val innerResults = inner.createResults(innerState)
        if (negated) innerResults.isEmpty else innerResults.nonEmpty
    }
  }

  private def name = if (negated) "AntiSemiApply" else "SemiApply"

  def planDescriptionWithoutCardinality =
    PlanDescriptionImpl(this.id, name, TwoChildren(source.planDescription, inner.planDescription), Seq.empty, identifiers)

  def symbols: SymbolTable = source.symbols

  override val sources = Seq(source, inner)

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: inner :: Nil) = sources
    copy(source = source, inner = inner)(estimatedCardinality)
  }

  override def localEffects = Effects()

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
