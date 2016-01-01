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
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.{PlanDescription, PlanDescriptionImpl, SingleChild}
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable

case class OptionalPipe(nullableIdentifiers: Set[String], source: Pipe)(implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {

  val notFoundExecutionContext: ExecutionContext =
    nullableIdentifiers.foldLeft(ExecutionContext.empty)( (context, identifier) => context += identifier -> null )

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    if (input.isEmpty) Iterator(notFoundExecutionContext) else input

  def planDescription: PlanDescription =
    new PlanDescriptionImpl(
      this,
      "Optional",
      SingleChild(source.planDescription),
      Seq.empty
    )

  def symbols: SymbolTable = source.symbols

  def dup(sources: List[Pipe]) = {
    val (head :: Nil) = sources
    copy(source = head)
  }

  override def localEffects = Effects.NONE
}
