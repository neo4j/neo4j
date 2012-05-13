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
package org.neo4j.cypher.internal.mutation

import org.neo4j.cypher.internal.symbols.Identifier
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb.PropertyContainer
import org.neo4j.cypher.internal.commands.{StartItem, Expression}
import org.neo4j.cypher.RelatePathNotUnique

case class RelateAction(links: RelateLink*) extends UpdateAction {
  def dependencies: Seq[Identifier] = links.flatMap(_.dependencies)

  def exec(context: ExecutionContext, state: QueryState): Traversable[ExecutionContext] = {
    var linksToDo: Seq[RelateLink] = links
    var ctx = context
    while (linksToDo.nonEmpty) {
      val results = executeAllRemainingPatterns(linksToDo, ctx, state)
      linksToDo = results.map(_._1)
      val updateCommands = extractUpdateCommands(results)
      val traversals = extractTraversals(results)

      if (results.isEmpty) {
        Stream(ctx) //We're done
      } else if (canNotAdvanced(results)) {
        throw new Exception("Unbound pattern!") //None of the patterns can advance. Fail.
      } else if (traversals.nonEmpty) {
        ctx = traverseNextStep(traversals, ctx) //We've found some way to move forward. Let's use it
      } else if (updateCommands.nonEmpty) {
        ctx = runUpdateCommands(updateCommands, ctx, state) //We could not move forward by traversing - let's build the road
      } else {
        throw new ThisShouldNotHappenError("Andres", "There was something in that result list I don't know how to handle.")
      }
    }

    Stream(ctx)
  }

  private def traverseNextStep(nextSteps: Seq[(String, PropertyContainer)], oldContext: ExecutionContext): ExecutionContext = {
    if (nextSteps.size != nextSteps.distinct.size) {
      //We can only go forward following a unique path. Fail.
      throw new RelatePathNotUnique("The pattern " + this + " produced multiple possible paths, and that is not allowed")
    } else {
      oldContext.newWith(nextSteps)
    }
  }

  private def runUpdateCommands(cmds: Seq[UpdateWrapper], oldContext: ExecutionContext, state: QueryState): ExecutionContext = {
    var context = oldContext
    var todo = cmds.distinct
    var done = Seq[String]()

    while (todo.nonEmpty) {
      val (unfiltered, temp) = todo.partition(_.canRun(context))
      todo = temp

      val current = unfiltered.filterNot(cmd => done.contains(cmd.cmd.identifierName))
      done = done ++ current.map(_.cmd.identifierName)

      context = current.foldLeft(context) {
        case (currentContext, updateCommand) => {
          val result = updateCommand.cmd.exec(currentContext, state)
          if (result.size != 1) {
            throw new RelatePathNotUnique("The pattern " + this + " produced multiple possible paths, and that is not allowed")
          } else {
            result.head
          }

        }
      }
    }

    context
  }

  private def extractUpdateCommands(results: scala.Seq[(RelateLink, RelateResult)]): Seq[UpdateWrapper] =
    results.flatMap {
      case (_, Update(cmds@_*)) => cmds
      case _ => None
    }

  private def extractTraversals(results: scala.Seq[(RelateLink, RelateResult)]): Seq[(String, PropertyContainer)] =
    results.flatMap {
      case (_, Traverse(ctx@_*)) => ctx
      case _ => None
    }

  private def executeAllRemainingPatterns(linksToDo: Seq[RelateLink], ctx: ExecutionContext, state: QueryState): Seq[(RelateLink, RelateResult)] =
    linksToDo.flatMap(link => {
      link.exec(ctx, state) match {
        case Done() => None
        case result => Some(link -> result)
      }
    })

  private def canNotAdvanced(results: scala.Seq[(RelateLink, RelateResult)]) = results.forall(_._2 == CanNotAdvance())

  def filter(f: (Expression) => Boolean): Seq[Expression] = links.flatMap(_.filter(f)).distinct

  def identifier: Seq[Identifier] = links.flatMap(_.identifier).distinct

  def rewrite(f: (Expression) => Expression): UpdateAction = RelateAction(links.map(_.rewrite(f)): _*)
}

sealed abstract class RelateResult

case class Done() extends RelateResult

case class CanNotAdvance() extends RelateResult

case class Traverse(result: (String, PropertyContainer)*) extends RelateResult

case class Update(cmds: UpdateWrapper*) extends RelateResult

case class UpdateWrapper(needs: Seq[String], cmd: StartItem with UpdateAction) {
  def canRun(context: ExecutionContext) = {
    lazy val keySet = context.keySet
    val forall = needs.forall(keySet.contains)
    forall
  }
}

