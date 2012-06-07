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
import org.neo4j.cypher.internal.pipes.{Pipe, ExecuteUpdateCommandsPipe, TransactionStartPipe}
import org.neo4j.cypher.internal.mutation.UpdateAction
import org.neo4j.cypher.internal.symbols.{NodeType, Identifier, SymbolTable}
import org.neo4j.cypher.internal.commands._
import collection.Map


class CreateNodesAndRelationshipsBuilder(db: GraphDatabaseService) extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val mutatingQueryTokens = q.start.filter(applicableTo(plan.pipe))
    val commands = mutatingQueryTokens.map(_.token.asInstanceOf[UpdateAction])
    val allCommands = expandCommands(commands, plan.pipe.symbols)

    val p = if (plan.containsTransaction) {
      plan.pipe
    } else {
      new TransactionStartPipe(plan.pipe, db)
    }

    val resultPipe = new ExecuteUpdateCommandsPipe(p, db, allCommands)
    val resultQuery = q.start.filterNot(mutatingQueryTokens.contains) ++ mutatingQueryTokens.map(_.solve)

    plan.copy(query = q.copy(start = resultQuery), pipe = resultPipe, containsTransaction = true)
  }

  private def expandCommands(commands: Seq[UpdateAction], symbols: SymbolTable): Seq[UpdateAction] = {
    val missingCreateNodeActions = commands.flatMap {
      case createNode: CreateNodeStartItem => Seq()
      case createRel: CreateRelationshipStartItem =>
        alsoCreateNode(createRel.from, symbols, commands) ++
          alsoCreateNode(createRel.to, symbols, commands)
      case x => Seq()
    }

    missingCreateNodeActions.distinct ++ commands
  }

  private def alsoCreateNode(e: (Expression, Map[String,Expression]), symbols: SymbolTable, commands: Seq[UpdateAction]): Seq[UpdateAction] = e._1 match {
    case Entity(name) =>
      val nodeFromUnderlyingPipe = symbols.satisfies(Seq(Identifier(name, NodeType())))
      val nodeFromOtherCommand = commands.exists {
        case CreateNodeStartItem(n, _) => n == name
        case _ => false
      }

      if (!nodeFromUnderlyingPipe && !nodeFromOtherCommand)
        Seq(CreateNodeStartItem(name, e._2))
      else
        Seq()

    case _ => Seq()
  }


  //key: String, from: Expression, to: Expression, typ: String, props: Map[String, Expression])

  def applicableTo(pipe: Pipe)(start: QueryToken[StartItem]) = start match {
    case Unsolved(x: CreateNodeStartItem) => pipe.symbols.satisfies(x.dependencies.toSeq)
    case Unsolved(x: CreateRelationshipStartItem) => pipe.symbols.satisfies(x.dependencies.toSeq)
    case _ => false
  }

  override def missingDependencies(plan: ExecutionPlanInProgress) = plan.query.start.flatMap {
    case Unsolved(x: CreateNodeStartItem) => plan.pipe.symbols.missingDependencies(x.dependencies.toSeq)
    case Unsolved(x: CreateRelationshipStartItem) => plan.pipe.symbols.missingDependencies(x.dependencies.toSeq)
    case _ => Seq()
  }.map(_.name)

  def canWorkWith(plan: ExecutionPlanInProgress) = plan.query.start.exists(applicableTo(plan.pipe))

  def priority = PlanBuilder.Mutation
}