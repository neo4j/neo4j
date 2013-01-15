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

import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.pipes.{Pipe, ExecuteUpdateCommandsPipe, TransactionStartPipe}
import org.neo4j.cypher.internal.mutation.UpdateAction
import org.neo4j.cypher.internal.commands.{UpdatingStartItem, StartItem}

class UpdateActionBuilder(db: GraphDatabaseService) extends PlanBuilder with UpdateCommandExpander {
  def apply(plan: ExecutionPlanInProgress) = {

    val p = if (plan.isUpdating) {
      plan.pipe
    } else {
      new TransactionStartPipe(plan.pipe, db)
    }

    val updateCmds: Seq[QueryToken[UpdateAction]] = extractValidUpdateActions(plan, p)
    val startItems: Seq[QueryToken[StartItem]] = extractValidStartItems(plan, p)
    val startCmds = startItems.map(_.map(_.asInstanceOf[UpdatingStartItem].updateAction))

    val updateActions = (startCmds ++ updateCmds).map(_.token)
    val commands = expandCommands(updateActions, p.symbols)

    val resultPipe = new ExecuteUpdateCommandsPipe(p, db, commands)

    plan.copy(
      isUpdating = true,
      query = plan.query.copy(
        updates = plan.query.updates.filterNot(updateCmds.contains) ++ updateCmds.map(_.solve),
        start = plan.query.start.filterNot(startItems.contains) ++ startItems.map(_.solve)),
      pipe = resultPipe
    )
  }

  private def extractValidStartItems(plan: ExecutionPlanInProgress, p: Pipe): Seq[QueryToken[StartItem]] = {
    plan.query.start.filter(cmd => cmd.unsolved && cmd.token.mutating && cmd.token.symbolDependenciesMet(p.symbols))
  }

  private def extractValidUpdateActions(plan: ExecutionPlanInProgress, p: Pipe): Seq[QueryToken[UpdateAction]] = {
    plan.query.updates.filter(cmd => cmd.unsolved && cmd.token.symbolDependenciesMet(p.symbols))
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val uas = extractValidUpdateActions(plan, plan.pipe).toSeq
    val sitems = extractValidStartItems(plan, plan.pipe).toSeq

    uas.nonEmpty || sitems.nonEmpty
  }

  def priority = PlanBuilder.Mutation

  override def missingDependencies(plan: ExecutionPlanInProgress): Seq[String] = {
    val updateDeps = plan.query.updates.flatMap {
      case Unsolved(cmd) => plan.pipe.symbols.missingSymbolTableDependencies(cmd)
      case _             => None
    }
    val startDeps = plan.query.start.flatMap {
      case Unsolved(cmd) if cmd.mutating => plan.pipe.symbols.missingSymbolTableDependencies(cmd)
      case _                             => None
    }

    (updateDeps ++ startDeps).
      distinct.
      map("Unknown identifier `%s`".format(_))
  }
}