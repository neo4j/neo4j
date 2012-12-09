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
import org.neo4j.cypher.internal.mutation.{CreateRelationship, CreateNode, UpdateAction}
import org.neo4j.cypher.internal.symbols.{NodeType, SymbolTable}
import org.neo4j.cypher.internal.commands._
import collection.Map
import collection.mutable
import expressions.{Identifier, Expression}
import org.neo4j.cypher.SyntaxException

class CreateNodesAndRelationshipsBuilder(db: GraphDatabaseService) extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val mutatingQueryTokens = q.start.filter(applicableTo(plan.pipe))

    val commands = mutatingQueryTokens.map(_.token.asInstanceOf[UpdatingStartItem].updateAction)
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
      case createRel: CreateRelationship =>
        alsoCreateNode(createRel.from, symbols, commands) ++
        alsoCreateNode(createRel.to, symbols, commands)
      case _                                      => Seq()
    }.distinct

    distinctify(missingCreateNodeActions) ++ commands
  }

  private def distinctify(nodes: Seq[CreateNode]): Seq[CreateNode] = {
    val createdNodes = mutable.Set[String]()
    nodes.flatMap {
      case CreateNode(key, props)
        if createdNodes.contains(key) && props.nonEmpty              =>
        throw new SyntaxException("Node `%s` has already been created. Can't assign properties to it again.".format(key))

      case CreateNode(key, _) if createdNodes.contains(key) => None

      case x@CreateNode(key, _)                             =>
        createdNodes += key
        Some(x)
    }
  }

  private def alsoCreateNode(e: (Expression, Map[String, Expression]), symbols: SymbolTable, commands: Seq[UpdateAction]): Seq[CreateNode] = e._1 match {
    case Identifier(name) =>
      val nodeFromUnderlyingPipe = symbols.checkType(name, NodeType())

      val nodeFromOtherCommand = commands.exists {
        case CreateNode(n, _) => n == name
        case _                         => false
      }

      if (!nodeFromUnderlyingPipe && !nodeFromOtherCommand)
        Seq(CreateNode(name, e._2))
      else
        Seq()

    case _ => Seq()
  }

  def applicableTo(pipe: Pipe)(start: QueryToken[StartItem]):Boolean = start match {
    case Unsolved(x: CreateNodeStartItem)         => x.checkTypes(pipe.symbols)
    case Unsolved(x: CreateRelationshipStartItem) => x.checkTypes(pipe.symbols)
    case _                                        => false
  }

  override def missingDependencies(plan: ExecutionPlanInProgress): Seq[String] = plan.query.start.flatMap {
    case Unsolved(x: CreateNodeStartItem)         => plan.pipe.symbols.missingSymbolTableDependencies(x)
    case Unsolved(x: CreateRelationshipStartItem) => plan.pipe.symbols.missingSymbolTableDependencies(x)
    case _                                        => Seq()
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = plan.query.start.exists(applicableTo(plan.pipe))

  def priority = PlanBuilder.Mutation
}