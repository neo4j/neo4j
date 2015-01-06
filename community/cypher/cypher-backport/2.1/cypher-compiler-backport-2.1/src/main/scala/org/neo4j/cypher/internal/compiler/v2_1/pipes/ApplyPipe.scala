/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.{PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable

case class ApplyPipe(source: Pipe, inner: Pipe)(implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.flatMap {
      (outerContext) =>
        val original = outerContext.clone()
        val innerState = state.copy(initialContext = Some(outerContext))
        val innerResults = inner.createResults(innerState)
        innerResults.map { context => context ++ original }
    }

  def planDescription =
    PlanDescriptionImpl(this, "Apply", TwoChildren(source.planDescription, inner.planDescription), Seq.empty)

  def symbols: SymbolTable = source.symbols.add(inner.symbols.identifiers)

  def dup(sources: List[Pipe]): Pipe = {
    val (l :: r :: Nil) = sources
    copy(source = l, inner= r)
  }

  override val sources: Seq[Pipe] = Seq(source, inner)

  override def localEffects = Effects.NONE
}
