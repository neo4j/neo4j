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
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.builders.{IfElseIterator, QueryStateSettingIterator}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{InternalPlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable

/**
 * This pipe does optional matches by making sure that the match pipe either finds a match for a start context,
 * or an execution context where all the introduced variables are now bound to null.
 */
case class OptionalMatchPipe(source: Pipe,
                             matchPipe: Pipe,
                             symbols: SymbolTable)
                            (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val listeningIterator = new QueryStateSettingIterator(input, state)

    new IfElseIterator(input = listeningIterator,
      ifClause = doMatch(state),
      elseClause = createNulls,
      finallyClause = () => state.initialContext = None)
  }

  def planDescription: InternalPlanDescription =
    PlanDescriptionImpl(this.id, "OptionalMatch", TwoChildren(source.planDescription, matchPipe.planDescription), Seq.empty, variables)

  val variablesBeforeMatch = matchPipe.symbols.variables.keySet
  val variablesAfterMatch = source.symbols.variables.keySet
  val introducedVariables = variablesBeforeMatch -- variablesAfterMatch
  val nulls: Map[String, Any] = introducedVariables.map(_ -> null).toMap

  private def createNulls(in: ExecutionContext): Iterator[ExecutionContext] = {
    Iterator(in.newWith(nulls))
  }

  override def localEffects = matchPipe.effects.leafEffectsAsOptional

  private def doMatch(state: QueryState)(ctx: ExecutionContext) = matchPipe.createResults(state)

  def dup(sources: List[Pipe]): Pipe = {
    val (source ::  Nil) = sources
    copy(source = source)
  }
}
