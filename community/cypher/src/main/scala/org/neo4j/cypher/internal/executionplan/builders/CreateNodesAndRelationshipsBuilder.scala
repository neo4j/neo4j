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

import org.neo4j.cypher.internal.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.pipes.{Pipe, ExecuteUpdateCommandsPipe}
import org.neo4j.cypher.internal.mutation._
import org.neo4j.cypher.internal.symbols.{CollectionType, AnyType, NodeType, SymbolTable}
import org.neo4j.cypher.internal.commands._
import collection.mutable
import expressions.Identifier
import org.neo4j.cypher.SyntaxException
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.commands.CreateNodeStartItem
import org.neo4j.cypher.internal.commands.CreateRelationshipStartItem
import org.neo4j.cypher.internal.mutation.CreateNode
import org.neo4j.cypher.internal.mutation.ForeachAction
import org.neo4j.cypher.internal.spi.PlanContext

class CreateNodesAndRelationshipsBuilder(db: GraphDatabaseService)
  extends PlanBuilder with UpdateCommandExpander with GraphElementPropertyFunctions {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext) = {
    val q = plan.query
    val mutatingQueryTokens = q.start.filter(applicableTo(plan.pipe))

    val commands = mutatingQueryTokens.map(_.token.asInstanceOf[UpdatingStartItem].updateAction)
    val allCommands = expandCommands(commands, plan.pipe.symbols)

    val resultPipe = new ExecuteUpdateCommandsPipe(plan.pipe, db, allCommands)
    val resultQuery = q.start.filterNot(mutatingQueryTokens.contains) ++ mutatingQueryTokens.map(_.solve)

    plan.copy(query = q.copy(start = resultQuery), pipe = resultPipe, isUpdating = true)
  }

  def applicableTo(pipe: Pipe)(start: QueryToken[StartItem]): Boolean = start match {
    case Unsolved(x: CreateNodeStartItem)         => x.symbolDependenciesMet(pipe.symbols)
    case Unsolved(x: CreateRelationshipStartItem) => x.symbolDependenciesMet(pipe.symbols)
    case _                                        => false
  }

  override def missingDependencies(plan: ExecutionPlanInProgress): Seq[String] = plan.query.start.flatMap {
    case Unsolved(x: CreateNodeStartItem)         => plan.pipe.symbols.missingSymbolTableDependencies(x)
    case Unsolved(x: CreateRelationshipStartItem) => plan.pipe.symbols.missingSymbolTableDependencies(x)
    case _                                        => Seq()
  }.map("Unknown identifier `%s`".format(_))

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) = plan.query.start.exists(applicableTo(plan.pipe))

  def priority = PlanBuilder.Mutation
}

trait UpdateCommandExpander {
  def expandCommands(commands: Seq[UpdateAction], symbols: SymbolTable): Seq[UpdateAction] = {
    def distinctify(nodes: Seq[UpdateAction]): Seq[UpdateAction] = {
      val createdNodes = mutable.Set[String]()

      nodes.flatMap { node =>
        node match {
          case CreateNode(key, props, _, _)
            if createdNodes.contains(key) && props.nonEmpty =>
            throw new SyntaxException("Node `%s` has already been created. Can't assign properties to it again.".format(key))

          case CreateNode(key, props, labels, bare)
            if !bare && createdNodes.contains(key) =>
            throw new SyntaxException("Node `%s` has already been created. Can't assign properties or labels to it again.".format(key))

          case CreateNode(key, _, _, _) if createdNodes.contains(key) =>
            None

          case x@CreateNode(key, _, _, _) =>
            createdNodes += key
            Some(x)

          case x =>
            Some(x)
        }
      }
    }

    def alsoCreateNode(e: RelationshipEndpoint, symbols: SymbolTable, commands: Seq[UpdateAction]): Seq[CreateNode] = e.node match {
      case Identifier(name) =>
        val nodeFromUnderlyingPipe = symbols.checkType(name, NodeType())

        val nodeFromOtherCommand = commands.exists {
          case CreateNode(n, _, _, _) => n == name
          case _                      => false
        }

        if (!nodeFromUnderlyingPipe && !nodeFromOtherCommand) {
          Seq(CreateNode(name, e.props, e.labels, e.bare))
        }
        else {
          Seq()
        }

      case _ =>
        Seq()
    }

    val missingCreateNodeActions = commands.flatMap {
      case ForeachAction(coll, id, actions) =>
        val expandedCommands = expandCommands(actions, symbols.add(id, coll.evaluateType(CollectionType(AnyType()), symbols).iteratedType))
        Seq(ForeachAction(coll, id, expandedCommands))

      case createRel: CreateRelationship =>
        alsoCreateNode(createRel.from, symbols, commands) ++
          alsoCreateNode(createRel.to, symbols, commands) ++ commands
      case _                             => commands
    }
    val distinctMissingCreateNodeActions = missingCreateNodeActions.distinct
    val distinctifiedCreateNodeActions = distinctify(distinctMissingCreateNodeActions)
    val sortedCreateNodeActions = new SortedUpdateActionIterator(distinctifiedCreateNodeActions, symbols).toSeq
    sortedCreateNodeActions
  }

  class SortedUpdateActionIterator(var commandsLeft: Seq[UpdateAction], var symbols: SymbolTable)
    extends Iterator[UpdateAction] {
    def hasNext = commandsLeft.nonEmpty

    def next() = {
      if (commandsLeft.isEmpty) {
        Iterator.empty.next()
      }
      else {

        //Let's get all the commands that are ready to run, and sort them so node creation happens before
        //relationship creation
        val nextCommands = commandsLeft.filter(action => action.symbolDependenciesMet(symbols)).sortWith {
          case (a: CreateNode, _) => true
          case (_, a: CreateNode) => false
          case _                  => false
        }

        if (nextCommands.isEmpty) {
          throw new ThisShouldNotHappenError("Andres", "This query should never have been built in the first place")
        }

        val head = nextCommands.head

        commandsLeft = commandsLeft.filterNot(_ == head)
        symbols = symbols.add(head.identifiers.toMap)
        head
      }
    }
  }

}