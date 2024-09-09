/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AddedInRewrite
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.bottomUp

import scala.annotation.tailrec

// We would want this to be part of a later phase, e.g. ASTRewriter.
// That is problematic because it needs to run expandStar, but the YIELD/WITH and RETURN clauses need scoping information.
// A solution would be to introduce another pass of SemanticAnalysis between this rewriter and expandStar.
// However, we really don't need more passes of SemanticAnalysis.
// It is not possible to modify or create scope at this point, to do so we would need to be a phase.
// Because of the restricted nature of SHOW commands being only `SHOW ... YIELD ... RETURN ...` this works for now,
// but we should revisit if we become more complicated/flexible.
case object rewriteShowQuery extends Step with DefaultPostCondition with PreparatoryRewritingRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // Introduces WITH * and RETURN *
    containsNoReturnAll,
    // It can invalidate this condition by introducing WITH and RETURN clauses
    ProjectionClausesHaveSemanticInfo
  )

  val instance: Rewriter = bottomUp(Rewriter.lift {
    case s @ SingleQuery(clauses) => s.copy(clauses = rewriteClauses(clauses.toList, List()))(s.position)
  })

  @tailrec
  private def rewriteClauses(clauses: List[Clause], rewrittenClause: List[Clause]): List[Clause] = clauses match {
    // Just a single command clause (with or without WHERE)
    case (commandClause: CommandClause) :: Nil =>
      rewrittenClause ++ rewriteToWithAndReturn(commandClause, commandClause.where)
    // Command clause with only a WITH (parsed as YIELD)
    case (commandClause: CommandClause) :: (withClause: With) :: Nil =>
      rewrittenClause :+ commandClause :+ updateDefaultOrderOnProjection(withClause, commandClause) :+
        returnClause(lastPosition(withClause), getDefaultOrderFromProjectionOrCommand(withClause, commandClause))
    // Command clause with WITH (parsed as YIELD) and RETURN * (to fix column order)
    case (commandClause: CommandClause) :: (withClause: With) :: (returnClause: Return) :: Nil
      if returnClause.returnItems.includeExisting =>
      rewrittenClause :+ commandClause :+ updateDefaultOrderOnProjection(
        withClause,
        commandClause
      ) :+ updateDefaultOrderOnReturn(
        returnClause,
        withClause,
        commandClause
      )
    case c :: cs => rewriteClauses(cs, rewrittenClause :+ c)
    case Nil     => rewrittenClause
  }

  private def getDefaultOrderFromProjectionOrCommand(projClause: ProjectionClause, commandClause: CommandClause) =
    if (projClause.returnItems.includeExisting)
      projClause.returnItems.defaultOrderOnColumns.getOrElse(commandClause.unfilteredColumns.columns.map(_.name))
    else projClause.returnItems.items.map(_.name).toList

  private def updateDefaultOrderOnReturn(
    returnClause: Return,
    projClause: ProjectionClause,
    commandClause: CommandClause
  ) = {
    val defaultOrderOnColumns = returnClause.returnItems.defaultOrderOnColumns.getOrElse(
      getDefaultOrderFromProjectionOrCommand(projClause, commandClause)
    )
    returnClause.withReturnItems(returnClause.returnItems.withDefaultOrderOnColumns(defaultOrderOnColumns))
  }

  private def updateDefaultOrderOnProjection(projClause: ProjectionClause, commandClause: CommandClause) = {
    val defaultOrderOnColumns = getDefaultOrderFromProjectionOrCommand(projClause, commandClause)
    projClause.copyProjection(returnItems = projClause.returnItems.withDefaultOrderOnColumns(defaultOrderOnColumns))
  }

  private def rewriteToWithAndReturn(commandClause: CommandClause, where: Option[Where]): List[Clause] = {
    val defaultColumnOrder = commandClause.unfilteredColumns.columns.map(_.name)
    List(
      commandClause.moveWhereToProjection,
      With(
        distinct = false,
        ReturnItems(includeExisting = true, Seq(), Some(defaultColumnOrder))(commandClause.position),
        None,
        None,
        None,
        where,
        withType = AddedInRewrite
      )(commandClause.position),
      returnClause(commandClause.position, defaultColumnOrder)
    )
  }

  private def returnClause(position: InputPosition, defaultOrderOnColumns: List[String]): Return =
    Return(ReturnItems(includeExisting = true, Seq(), Some(defaultOrderOnColumns))(position))(position)
      .copy(addedInRewrite = true)(position)

  private def lastPosition(c: ProjectionClause): InputPosition = {
    c.folder.treeFold(InputPosition.NONE) {
      case node: ASTNode => acc => TraverseChildren(Seq(acc, node.position).max)
    }
  }

  override def getRewriter(
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = instance
}
