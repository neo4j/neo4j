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

import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.pipes.{ExecuteUpdateCommandsPipe, TransactionStartPipe}

class DeleteAndPropertySetBuilder(db: GraphDatabaseService) extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {

    val p = if (plan.containsTransaction) {
      plan.pipe
    } else {
      new TransactionStartPipe(plan.pipe, db)
    }

    val commands = plan.query.updates.filter(cmd => cmd.unsolved && p.symbols.satisfies(cmd.token.dependencies))
    val resultPipe = new ExecuteUpdateCommandsPipe(p, db, commands.map(_.token))

    plan.copy(
      containsTransaction = true,
      query = plan.query.copy(updates = plan.query.updates.filterNot(commands.contains) ++ commands.map(_.solve)),
      pipe = resultPipe
    )
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = plan.query.updates.exists(cmd => cmd.unsolved && plan.pipe.symbols.satisfies(cmd.token.dependencies))

  def priority = PlanBuilder.Mutation
}