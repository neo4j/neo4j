/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import collection.Seq
import GetGraphElements.getElements
import org.neo4j.cypher.internal.executionplan._
import org.neo4j.cypher.internal.commands.{RelationshipById, StartItem}
import org.neo4j.graphdb.{Relationship, GraphDatabaseService}
import org.neo4j.cypher.internal.pipes.RelationshipStartPipe

class RelationshipByIdBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def priority = PlanBuilder.RelationshipById

  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe
    val startItemToken = interestingStartItems(q).head
    val Unsolved(RelationshipById(key, expression)) = startItemToken

    val pipe = new RelationshipStartPipe(p, key, m => getElements[Relationship](expression(m), key, graph.getRelationshipById))

    val remainingQ: Seq[QueryToken[StartItem]] = q.start.filterNot(_ == startItemToken) :+ startItemToken.solve

    plan.copy(pipe = pipe, query = q.copy(start = remainingQ))
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = interestingStartItems(plan.query).nonEmpty

  private def interestingStartItems(q: PartiallySolvedQuery): Seq[QueryToken[StartItem]] = q.start.filter({
    case Unsolved(RelationshipById(_, expression)) => true
    case _ => false
  })
}