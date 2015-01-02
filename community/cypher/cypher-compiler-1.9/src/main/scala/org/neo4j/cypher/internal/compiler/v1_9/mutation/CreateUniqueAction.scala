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
package org.neo4j.cypher.internal.compiler.v1_9.mutation

import org.neo4j.cypher.internal.compiler.v1_9.symbols.{CypherType, SymbolTable}
import org.neo4j.cypher.internal.compiler.v1_9.pipes.{QueryState}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v1_9.commands.StartItem
import org.neo4j.cypher.UniquePathNotUniqueException
import org.neo4j.graphdb.{Lock, PropertyContainer}
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext

case class CreateUniqueAction(incomingLinks: UniqueLink*) extends UpdateAction {

  def exec(context: ExecutionContext, state: QueryState): Traversable[ExecutionContext] = {
    var linksToDo: Seq[UniqueLink] = links
    var ctx = context
    while (linksToDo.nonEmpty) {
      val results: Seq[(UniqueLink, CreateUniqueResult)] = executeAllRemainingPatterns(linksToDo, ctx, state)
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
        val locks = updateCommands.flatMap(_.lock()) //Failed to find a way forward - lock stuff up, and check again
        try {
          ctx = tryAgain(linksToDo, ctx, state)
        } finally {
          locks.foreach(_.release())
        }
      } else {
        throw new ThisShouldNotHappenError("Andres", "There was something in that result list I don't know how to handle.")
      }
    }

    Stream(ctx)
  }

  override def addsToRow(): Seq[String] = Seq()

  /**
   * Here we take the incoming links and prepare them to be used, by making sure that
   * no named expectations contradict each other
   */
  val links:Seq[UniqueLink] = {
    val nodesWithProperties: Seq[NamedExpectation] = incomingLinks.flatMap(_.nodesWProps)

    nodesWithProperties.foldLeft(incomingLinks) {
      case (bunchOfLinks, nodeExpectation) => bunchOfLinks.map(link => link.expect(nodeExpectation))
    }
  }

  private def tryAgain(linksToDo: Seq[UniqueLink], context: ExecutionContext, state: QueryState): ExecutionContext = {
    val results: Seq[(UniqueLink, CreateUniqueResult)] = executeAllRemainingPatterns(linksToDo, context, state)
    val updateCommands = extractUpdateCommands(results)
    val traversals = extractTraversals(results)

    if (results.isEmpty) {
      throw new ThisShouldNotHappenError("Andres", "Second check should never return empty result set")
    } else if (canNotAdvanced(results)) {
      throw new ThisShouldNotHappenError("Andres", "Second check should never fail to move forward")
    } else if (traversals.nonEmpty) {
      traverseNextStep(traversals, context) //Ah, so this time we did find a traversal way forward. Great!
    } else if (updateCommands.nonEmpty) {
      runUpdateCommands(updateCommands.flatMap(_.cmds), context, state) //If we still can't find a way forward,
    } else {                                                            // let's build one
      throw new ThisShouldNotHappenError("Andres", "There was something in that result list I don't know how to handle.")
    }
  }

  case class TraverseResult(identifier: String, element: PropertyContainer, link: UniqueLink)

  private def traverseNextStep(nextSteps: Seq[TraverseResult], oldContext: ExecutionContext): ExecutionContext = {
    val uniqueKVPs = nextSteps.map(x => x.identifier -> x.element).distinct
    val uniqueKeys = uniqueKVPs.toMap

    if (uniqueKeys.size != uniqueKVPs.size) {
      fail(nextSteps)
    } else {
      oldContext.newWith(uniqueKeys)
    }
  }

  private def fail(nextSteps: Seq[TraverseResult]): Nothing = {
    //We can only go forward following a unique path. Fail.
    val problemResultsByIdentifier: Map[String, Seq[TraverseResult]] = nextSteps.groupBy(_.identifier).
      filter(_._2.size > 1)

    val message = problemResultsByIdentifier.map {
      case (identifier, links: Seq[TraverseResult]) =>
        val hits = links.map(result => "%s found by : %s".format(result.element, result.link))

        "Nodes for identifier: `%s` were found with differing values by these pattern relationships: %s".format(identifier, hits.mkString("\n  ", "\n  ", "\n"))
    }

    throw new UniquePathNotUniqueException(message.mkString("CREATE UNIQUE error\n", "\n", "\n"))
  }

  private def runUpdateCommands(cmds: Seq[UpdateWrapper], oldContext: ExecutionContext, state: QueryState): ExecutionContext = {
    var context = oldContext
    var todo = cmds.distinct
    var done = Seq[String]()

    while (todo.nonEmpty) {
      val (unfiltered, temp) = todo.partition(_.canRun(context))
      todo = temp

      val current = unfiltered.filterNot(cmd => done.contains(cmd.key))
      done = done ++ current.map(_.key)

      context = current.foldLeft(context) {
        case (currentContext, updateCommand) => {
          val result = updateCommand.cmd.exec(currentContext, state)
          if (result.size != 1) {
            throw new UniquePathNotUniqueException("The pattern " + this + " produced multiple possible paths, and that is not allowed")
          } else {
            result.head
          }

        }
      }
    }

    context
  }

  private def extractUpdateCommands(results: scala.Seq[(UniqueLink, CreateUniqueResult)]): Seq[Update] =
    results.flatMap {
      case (_, u: Update) => Some(u)
      case _ => None
    }

  private def extractTraversals(results: scala.Seq[(UniqueLink, CreateUniqueResult)]): Seq[TraverseResult] =
    results.flatMap {
      case (link, Traverse(ctx@_*)) => ctx.map {
        case (key, element) => TraverseResult(key, element, link)
      }
      case _                        => None
    }

  private def executeAllRemainingPatterns(linksToDo: Seq[UniqueLink], ctx: ExecutionContext, state: QueryState): Seq[(UniqueLink, CreateUniqueResult)] = linksToDo.flatMap(link => link.exec(ctx, state))

  private def canNotAdvanced(results: scala.Seq[(UniqueLink, CreateUniqueResult)]) = results.forall(_._2 == CanNotAdvance())

  override def children = links.flatMap(_.children)

  def identifiers: Seq[(String,CypherType)] = links.flatMap(_.identifier2).distinct

  override def rewrite(f: (Expression) => Expression) = CreateUniqueAction(links.map(_.rewrite(f)): _*)

  def throwIfSymbolsMissing(symbols: SymbolTable) {links.foreach(l=>l.throwIfSymbolsMissing(symbols))}

  override def symbolTableDependencies = links.flatMap(_.symbolTableDependencies).toSet
}

sealed abstract class CreateUniqueResult

case class CanNotAdvance() extends CreateUniqueResult

case class Traverse(result: (String, PropertyContainer)*) extends CreateUniqueResult

case class Update(cmds: Seq[UpdateWrapper], locker: () => Seq[Lock]) extends CreateUniqueResult {
  def lock(): Seq[Lock] = locker()
}

case class UpdateWrapper(needs: Seq[String], cmd: UpdateAction, key: String) {
  def canRun(context: ExecutionContext) = {
    lazy val keySet = context.keySet
    val forall = needs.forall(keySet.contains)
    forall
  }
}

