/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{ExecutionPlanInProgress, Phase, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.{PlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable

case class OptionalMatchBuilder(solveMatch: Phase) extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): Boolean = plan.query.optional

  def apply(in: ExecutionPlanInProgress, context: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
    val listeningPipe = new NullPipe(in.pipe.symbols) {
      override def planDescription: PlanDescription = in.pipe.planDescription
    }
    val nonOptionalQuery = in.query.copy(optional = false)
    val postMatchPlan = solveMatch(in.copy(pipe = listeningPipe, query = nonOptionalQuery), context)
    val matchPipe = postMatchPlan.pipe

    val optionalMatchPipe = OptionalMatchPipe(in.pipe, matchPipe, matchPipe.symbols)
    postMatchPlan.copy(pipe = optionalMatchPipe)
  }

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

    def planDescription: PlanDescription =
    PlanDescriptionImpl(this, "OptionalMatch", TwoChildren(source.planDescription, matchPipe.planDescription), Seq.empty)

    val identifiersBeforeMatch = matchPipe.symbols.identifiers.map(_._1).toSet
    val identifiersAfterMatch = source.symbols.identifiers.map(_._1).toSet
    val introducedIdentifiers = identifiersBeforeMatch -- identifiersAfterMatch
    val nulls: Map[String, Any] = introducedIdentifiers.map(_ -> null).toMap

    private def createNulls(in: ExecutionContext): Iterator[ExecutionContext] = {
      Iterator(in.newWith(nulls))
    }

    def doMatch(state: QueryState)(ctx: ExecutionContext) = matchPipe.createResults(state)

    def dup(sources: List[Pipe]): Pipe = {
      val (l :: r :: Nil) = sources
      copy(source = l, matchPipe = r)
    }

    override val sources: Seq[Pipe] = Seq(source, matchPipe)
  }
}
