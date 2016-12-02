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
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Id, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable

case class ConditionalApplyPipe(source: Pipe, inner: Pipe, items: Seq[String], negated: Boolean)
                               (val estimatedCardinality: Option[Double] = None, val id: Id = new Id)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.flatMap {
      (outerContext) =>
        if (condition(outerContext)) {
          val original = outerContext.clone()
          val innerState = state.withInitialContext(outerContext)
          val innerResults = inner.createResults(innerState)
          innerResults.map { context => original ++ context }
        } else Iterator.single(outerContext)
    }

  private def condition(context: ExecutionContext) = {
    val cond = items.exists { context.get(_).get != null}
      if (negated) !cond else cond
  }

  private def name = if (negated) "AntiConditionalApply" else "ConditionalApply"

  def planDescriptionWithoutCardinality =
    PlanDescriptionImpl(this.id, name, TwoChildren(source.planDescription, inner.planDescription), Seq.empty, variables)

  def symbols: SymbolTable = source.symbols.add(inner.symbols.variables)

  def dup(sources: List[Pipe]): Pipe = {
    val (l :: r :: Nil) = sources
    copy(source = l, inner= r)(estimatedCardinality, id)
  }

  override val sources: Seq[Pipe] = Seq(source, inner)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated), id)
}
