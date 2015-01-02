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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan.builders

import org.neo4j.cypher.internal.compiler.v1_9.pipes.{MatchPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v1_9.commands._
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v1_9.commands.ShortestPath

class MatchBuilder extends PlanBuilder with PatternGraphBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe

    val items = q.patterns.filter(yesOrNo(_, p, q.start))
    val patterns = items.map(_.token)
    val predicates = q.where.filter(!_.solved).map(_.token)
    val graph = buildPatternGraph(p.symbols, patterns)

    val mandatoryGraph = graph.mandatoryGraph

    val mandatoryPipe = if (mandatoryGraph.nonEmpty)
      new MatchPipe(p, predicates, mandatoryGraph)
    else
      p

    // We'll create one MatchPipe per DoubleOptionalPattern we have
    val newPipe = graph.doubleOptionalPatterns().
      foldLeft(mandatoryPipe)((lastPipe, patternGraph) => new MatchPipe(lastPipe, predicates, patternGraph))

    val donePatterns = graph.patternsContained.map(Unsolved.apply)

    plan.copy(
      query = q.copy(patterns = q.patterns.filterNot(donePatterns.contains) ++ donePatterns.map(_.solve)),
      pipe = newPipe
    )
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    q.patterns.exists(yesOrNo(_, plan.pipe, q.start))
  }

  private def yesOrNo(q: QueryToken[_], p: Pipe, start: Seq[QueryToken[StartItem]]) = q match {
    case Unsolved(x: ShortestPath) => false
    case Unsolved(x: Pattern) => {
      val patternIdentifiers: Seq[String] = x.possibleStartPoints.map(_._1)

      val apa = start.map(si => patternIdentifiers.find(_ == si.token.identifierName) match {
        case Some(_) => si.solved
        case None => true
      })

      val resolvedStartPoints =  apa.foldLeft(true)(_ && _)
      val pipeSatisfied = x.predicate.symbolDependenciesMet(p.symbols)

      resolvedStartPoints && pipeSatisfied
    }
    case _ => false
  }

  def priority = PlanBuilder.Match
}
