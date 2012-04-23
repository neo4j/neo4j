/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.pipes.{MatchPipe, Pipe}
import org.neo4j.cypher.internal.commands.{ShortestPath, StartItem, Pattern}
import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}

class MatchBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe


    val items = q.patterns.filter(yesOrNo(_, p, q.start))
    val patterns = items.map(_.token)
    val predicates = q.where.filter(!_.solved).map(_.token)

    val newPipe = new MatchPipe(p, patterns, predicates)

    plan.copy(
      query = q.copy(patterns = q.patterns.filterNot(items.contains) ++ items.map(_.solve)),
      pipe = newPipe
    )
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    q.patterns.filter(yesOrNo(_, plan.pipe, q.start)).nonEmpty
  }

  private def yesOrNo(q: QueryToken[_], p: Pipe, start: Seq[QueryToken[StartItem]]) = q match {
    case Unsolved(x: ShortestPath) => false
    case Unsolved(x: Pattern) => {

      val resolvedStartPoints = start.map(si => x.possibleStartPoints.find(_.name == si.token.identifierName) match {
        case Some(_) => si.solved
        case None => true
      }).foldLeft(true)(_ && _)

      lazy val pipeSatisfied = p.symbols.satisfies(x.predicate.dependencies)

      resolvedStartPoints && pipeSatisfied
    }
    case _ => false
  }

  def priority = PlanBuilder.Match
}