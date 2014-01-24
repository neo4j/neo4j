/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._

sealed trait Clause extends AstNode with SemanticCheckable {
  def name: String
}

sealed trait UpdateClause extends Clause

sealed trait ClosingClause extends Clause {
  def distinct: Boolean
  def returnItems: ReturnItems
  def orderBy: Option[OrderBy]
  def skip: Option[Skip]
  def limit: Option[Limit]

  def semanticCheck =
    returnItems.semanticCheck then
    checkSortItems then
    checkSkipLimit

  // use a scoped state containing the aliased return items for the sort expressions
  private def checkSortItems: SemanticCheck = s => {
    val result = (returnItems.declareIdentifiers(s) then orderBy.semanticCheck)(s.newScope)
    SemanticCheckResult(result.state.popScope, result.errors)
  }

  // use an empty state when checking skip & limit, as these have isolated scope
  private def checkSkipLimit: SemanticState => Seq[SemanticError] =
    s => (skip ++ limit).semanticCheck(SemanticState.clean).errors
}


case class Start(items: Seq[StartItem], where: Option[Where])(val token: InputToken) extends Clause {
  val name = "START"

  def semanticCheck = items.semanticCheck then where.semanticCheck
}

case class Match(optional: Boolean, pattern: Pattern, hints: Seq[Hint], where: Option[Where])(val token: InputToken) extends Clause with SemanticChecking {
  def name = "MATCH"

  def semanticCheck =
      pattern.semanticCheck(Pattern.SemanticContext.Match) then
      hints.semanticCheck then
      where.semanticCheck
}

case class Merge(pattern: Pattern, actions: Seq[MergeAction])(val token: InputToken) extends UpdateClause {
  def name = "MERGE"

  def semanticCheck =
    pattern.semanticCheck(Pattern.SemanticContext.Merge) then
    actions.semanticCheck
}

case class Create(pattern: Pattern)(val token: InputToken) extends UpdateClause {
  def name = "CREATE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Create)
}

case class CreateUnique(pattern: Pattern)(val token: InputToken) extends UpdateClause {
  def name = "CREATE UNIQUE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Create)
}

case class SetClause(items: Seq[SetItem])(val token: InputToken) extends UpdateClause {
  def name = "SET"

  def semanticCheck = items.semanticCheck
}

case class Delete(expressions: Seq[Expression])(val token: InputToken) extends UpdateClause {
  def name = "DELETE"

  def semanticCheck =
    expressions.semanticCheck(Expression.SemanticContext.Simple) then
    warnAboutDeletingLabels then
    expressions.expectType(CTNode.covariant | CTRelationship.covariant | CTPath.covariant)

  def warnAboutDeletingLabels =
    expressions.filter(_.isInstanceOf[HasLabels]) map {
      e => SemanticError("DELETE doesn't support removing labels from a node. Try REMOVE.", e.token)
    }
}

case class Remove(items: Seq[RemoveItem])(val token: InputToken) extends UpdateClause {
  def name = "REMOVE"

  def semanticCheck = items.semanticCheck
}

case class Foreach(identifier: Identifier, expression: Expression, updates: Seq[Clause])(val token: InputToken) extends UpdateClause with SemanticChecking {
  def name = "FOREACH"

  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) then
    expression.expectType(CTCollection(CTAny).covariant) then withScopedState {
      val possibleInnerTypes: TypeGenerator = expression.types(_).unwrapCollections
      identifier.declare(possibleInnerTypes) then updates.semanticCheck
    } then updates.filter(!_.isInstanceOf[UpdateClause]).map(c => SemanticError(s"Invalid use of ${c.name} inside FOREACH", c.token))
}

case class With(
    distinct: Boolean,
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    where: Option[Where])(val token: InputToken) extends ClosingClause
{
  def name = "WITH"

  override def semanticCheck =
    super.semanticCheck then
    checkAliasedReturnItems

  private def checkAliasedReturnItems: SemanticState => Seq[SemanticError] = state => returnItems match {
    case li: ListedReturnItems => li.items.filter(!_.alias.isDefined).map(i => SemanticError("Expression in WITH must be aliased (use AS)", i.token))
    case _                     => Seq()
  }
}

case class Return(
    distinct: Boolean,
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit])(val token: InputToken) extends ClosingClause {
  def name = "RETURN"
}
