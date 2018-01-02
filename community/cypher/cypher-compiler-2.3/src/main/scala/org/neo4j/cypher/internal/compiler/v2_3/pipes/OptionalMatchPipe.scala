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
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.{IfElseIterator, QueryStateSettingIterator}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{InternalPlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable

/**
 * This pipe does optional matches by making sure that the match pipe either finds a match for a start context,
 * or an execution context where all the introduced identifiers are now bound to null.
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
    PlanDescriptionImpl(this.id, "OptionalMatch", TwoChildren(source.planDescription, matchPipe.planDescription), Seq.empty, identifiers)

  val identifiersBeforeMatch = matchPipe.symbols.identifiers.map(_._1).toSet
  val identifiersAfterMatch = source.symbols.identifiers.map(_._1).toSet
  val introducedIdentifiers = identifiersBeforeMatch -- identifiersAfterMatch
  val nulls: Map[String, Any] = introducedIdentifiers.map(_ -> null).toMap

  private def createNulls(in: ExecutionContext): Iterator[ExecutionContext] = {
    Iterator(in.newWith(nulls))
  }

  override def localEffects = matchPipe.localEffects

  private def doMatch(state: QueryState)(ctx: ExecutionContext) = matchPipe.createResults(state)

  def dup(sources: List[Pipe]): Pipe = {
    val (source ::  Nil) = sources
    copy(source = source)
  }
}
