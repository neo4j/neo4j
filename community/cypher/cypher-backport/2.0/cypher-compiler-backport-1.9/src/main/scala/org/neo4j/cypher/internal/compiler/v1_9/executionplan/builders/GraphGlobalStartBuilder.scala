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

import org.neo4j.cypher.internal.compiler.v1_9.commands._
import org.neo4j.cypher.internal.compiler.v1_9.pipes.{RelationshipStartPipe, NodeStartPipe, Pipe}
import org.neo4j.graphdb.GraphDatabaseService
import collection.JavaConverters._
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery, PlanBuilder}

class GraphGlobalStartBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe
    val item = q.start.filter(filter).head

    val newPipe = createStartPipe(p, item.token)

    val newQ: PartiallySolvedQuery = q.copy(start = q.start.filterNot(_ == item) :+ item.solve)

    plan.copy(pipe = newPipe, query = newQ)
  }

  private def filter(q: QueryToken[_]) = q match {
    case Unsolved(AllNodes(_)) => true
    case Unsolved(AllRelationships(_)) => true
    case _ => false
  }

  private def createStartPipe(lastPipe: Pipe, item: StartItem): Pipe =
    item match {
    case AllNodes(identifierName) => new NodeStartPipe(lastPipe, identifierName, (ctx,state) => state.query.nodeOps.all)
    case AllRelationships(identifierName) => new RelationshipStartPipe(lastPipe, identifierName, (ctx, state) => state.query.relationshipOps.all)
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = plan.query.start.exists(filter)

  def priority = PlanBuilder.GlobalStart
}
