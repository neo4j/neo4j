/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.PropertySupport
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{MatchPipe, Pipe, QueryState}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.MergePattern
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, InvalidSemanticsException}
import org.neo4j.graphdb.Node
import org.neo4j.helpers.ThisShouldNotHappenError

case class MergePatternAction(patterns: Seq[Pattern],
                              actions: Seq[UpdateAction],
                              onCreate: Seq[SetAction],
                              onMatch: Seq[SetAction],
                              maybeUpdateActions: Option[Seq[UpdateAction]] = None,
                              maybeMatchPipe: Option[Pipe] = None) extends UpdateAction {

  def children: Seq[AstNode[_]] = patterns ++ actions ++ onCreate ++ onMatch

  def readyToExecute = maybeMatchPipe.nonEmpty && maybeUpdateActions.nonEmpty

  def readyToUpdate(symbols: SymbolTable) = !readyToExecute && symbolDependenciesMet(symbols)

  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] = {
    state.initialContext = Some(context)

    // TODO get rid of double evaluation of property expressions
    MergePatternAction.ensureNoNullRelationshipPropertiesInPatterns(patterns, context, state)

    val matchResult = doMatch(state).toList
    if(matchResult.nonEmpty)
      return matchResult.iterator

    val lockedMatchResult = lockAndThenMatch(state, context)
    if(lockedMatchResult.nonEmpty)
      return lockedMatchResult

    createThePattern(state, context)
  }

  private def matchPipe: Pipe =
    maybeMatchPipe.getOrElse(throw new InternalException("Query not prepared correctly!"))

  private def updateActions(): Seq[UpdateAction] =
    maybeUpdateActions.getOrElse(throw new InternalException("Query not prepared correctly!"))

  private def doMatch(state: QueryState) = matchPipe.createResults(state)

  private def lockAndThenMatch(state: QueryState, ctx: ExecutionContext): Iterator[ExecutionContext] = {
    val lockingQueryContext = state.query.upgradeToLockingQueryContext
    val patternIdentifiers = identifiers.map(p => p._1)
    ctx.collect { case (identifier, node: Node) if patternIdentifiers.contains(identifier) => node.getId }.toSeq.sorted.
      foreach( id => lockingQueryContext.getLabelsForNode(id) ) // TODO: This locks the nodes. Hack!
    matchPipe.createResults(state)
  }

  private def createThePattern(state: QueryState, ctx: ExecutionContext) = {
    // Runs all commands, from left to right, updating the execution context as it passes through
    val resultingContext = updateActions().foldLeft(ctx) {
      case (accumulatedContext, action) => singleElementOrFail(action.exec(accumulatedContext, state))
    }
    Iterator(resultingContext)
  }

  private def singleElementOrFail(inner: Iterator[ExecutionContext]): ExecutionContext = {
    val temp = inner.next()

    if (inner.hasNext)
      throw new InternalException("Creating a missing element in MERGE resulted in multiple elements")

    temp
  }

  def identifiers: Seq[(String, CypherType)] = patterns.flatMap(_.possibleStartPoints)

  def rewrite(f: (Expression) => Expression): UpdateAction =
    MergePatternAction(
      patterns = patterns.map(_.rewrite(f)),
      actions = actions.map(_.rewrite(f)),
      onCreate = onCreate.map(_.rewrite(f)),
      onMatch = onMatch.map(_.rewrite(f)),
      maybeUpdateActions = maybeUpdateActions.map(_.map(_.rewrite(f))),
      maybeMatchPipe = maybeMatchPipe)

  def symbolTableDependencies: Set[String] = {
    val dependencies = (
      patterns.flatMap(_.symbolTableDependencies) ++
      actions.flatMap(_.symbolTableDependencies) ++
      onCreate.flatMap(_.symbolTableDependencies) ++
      onMatch.flatMap(_.symbolTableDependencies)).toSet

    val introducedIdentifiers = patterns.flatMap(_.identifiers).toSet

    dependencies -- introducedIdentifiers
  }

  private def readEffects(symbols: SymbolTable): Effects = {
    val collect: Seq[Effect] = identifiers.collect {
      case (k, CTNode) if !symbols.hasIdentifierNamed(k) => ReadsAllNodes
      case (k, CTRelationship) if !symbols.hasIdentifierNamed(k) => ReadsRelationships
    }

    Effects(collect.toSet)
  }

  def localEffects(externalSymbols: SymbolTable) = {
    import Effects._

    val effectsFromReading = readEffects(externalSymbols)

    val allSymbols = updateSymbols(externalSymbols)
    val actionEffects = actions.effects(allSymbols)
    val onCreateEffects = onCreate.effects(allSymbols)
    val onMatchEffects = onMatch.effects(allSymbols)
    val updateActionsEffects = updateActions().effects(allSymbols)

    actionEffects | onCreateEffects | onMatchEffects | updateActionsEffects | effectsFromReading
  }

  override def updateSymbols(symbol: SymbolTable): SymbolTable = symbol.add(identifiers.toMap)

  override def arguments: Seq[Argument] = {
    val startPoint: Option[String] = maybeMatchPipe.map {
      case m: MatchPipe => m.mergeStartPoint
      case _ => ""
    }
    Seq(MergePattern(startPoint.getOrElse("")))
  }

}

object MergePatternAction {

  def ensureNoNullRelationshipPropertiesInPatterns(patterns: Seq[Pattern], context: ExecutionContext, state: QueryState) {
    for (pattern <- patterns) {
      val extractedProperties = pattern match {
        case RelatedTo(_, _, _, _, _, properties) => Some(properties)
        case VarLengthRelatedTo(_, _, _, _, _, _, _, _, _)
             | UniqueLink(_, _, _, _, _) =>
          throw new ThisShouldNotHappenError("Davide/Stefan", "Merge patterns do not support var length or unique link")
        case _ => None
      }

      val nullProperty = extractedProperties.flatMap(PropertySupport.firstNullPropertyIfAny(_, context, state))

      nullProperty match {
        case Some(key) =>
          throw new InvalidSemanticsException(s"Cannot merge relationship using null property value for $key")
        case None =>
        // awesome
      }
    }
  }
}
