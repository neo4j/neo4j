/**
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.{Expression, NumericHelper}
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.LegacyExpression
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable

case class SkipPipe(source: Pipe, exp: Expression)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with NumericHelper {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if(input.isEmpty)
      return Iterator.empty

    implicit val s = state

    val first: ExecutionContext = input.next()

    val count = asInt(exp(first))

    new HeadAndTail(first, input).drop(count)
  }

  override def planDescription = source
    .planDescription
    .andThen(this, "Skip", LegacyExpression(exp))

  def symbols: SymbolTable = source.symbols

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }

  override def localEffects = exp.effects
}
