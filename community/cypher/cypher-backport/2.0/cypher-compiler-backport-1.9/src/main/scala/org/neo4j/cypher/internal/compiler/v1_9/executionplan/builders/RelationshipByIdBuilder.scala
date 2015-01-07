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

import collection.Seq
import GetGraphElements.getElements
import org.neo4j.cypher.internal.compiler.v1_9.executionplan._
import org.neo4j.cypher.internal.compiler.v1_9.commands.{RelationshipById, StartItem}
import org.neo4j.graphdb.{Node, Relationship, GraphDatabaseService}
import org.neo4j.cypher.internal.compiler.v1_9.pipes.{QueryState, RelationshipStartPipe}
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext

class RelationshipByIdBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def priority = PlanBuilder.RelationshipById

  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe
    val startItemToken = interestingStartItems(q).head
    val Unsolved(RelationshipById(key, expression)) = startItemToken

    def f(ctx: ExecutionContext, state: QueryState) =
      getElements[Relationship](expression(ctx)(state), key, state.query.relationshipOps.getById)

    val pipe = new RelationshipStartPipe(p, key, f)

    val remainingQ: Seq[QueryToken[StartItem]] = q.start.filterNot(_ == startItemToken) :+ startItemToken.solve

    plan.copy(pipe = pipe, query = q.copy(start = remainingQ))
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = interestingStartItems(plan.query).nonEmpty

  private def interestingStartItems(q: PartiallySolvedQuery): Seq[QueryToken[StartItem]] = q.start.filter({
    case Unsolved(RelationshipById(_, expression)) => true
    case _ => false
  })
}
