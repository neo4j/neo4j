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
import commands.{expressions => commandexpressions}
import org.neo4j.cypher.internal.compiler.v2_0.mutation.{CreateNode, UpdateAction, ForeachAction}
import symbols._
import org.neo4j.helpers.ThisShouldNotHappenError

sealed trait Clause extends AstNode with SemanticCheckable {
  def name: String
}

sealed trait UpdateClause extends Clause {
  def legacyUpdateActions: Seq[UpdateAction]
}

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

  def closeLegacyQueryBuilder(builder: commands.QueryBuilder): commands.Query = {
    val returns = returnItems.toCommands

    val addAggregates = (b: commands.QueryBuilder) => extractAggregationExpressions(returns).fold(b) { b.aggregation(_:_*) }
    val addSkip = (b: commands.QueryBuilder) => skip.fold(b) { s => b.skip(s.toCommand) }
    val addLimit = (b: commands.QueryBuilder) => limit.fold(b) { l => b.limit(l.toCommand) }
    val addOrder = (b: commands.QueryBuilder) => orderBy.fold(b) { o => b.orderBy(o.sortItems.map(_.toCommand):_*) }

    (
      addAggregates andThen
      addSkip andThen
      addLimit andThen
      addOrder
    )(builder).returns(returns:_*)
  }

  protected def extractAggregationExpressions(items: Seq[commands.ReturnColumn]) = {
    val aggregationExpressions = items.collect {
      case commands.ReturnItem(expression, _, _) => (expression.subExpressions :+ expression).collect {
        case agg: commandexpressions.AggregationExpression => agg
      }
    }.flatten

    (aggregationExpressions, distinct) match {
      case (Seq(), false) => None
      case _              => Some(aggregationExpressions)
    }
  }
}


case class Start(items: Seq[StartItem], where: Option[Where])(val token: InputToken) extends Clause {
  val name = "START"

  def semanticCheck = items.semanticCheck then where.semanticCheck

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val startItems = items.map(_.toCommand)
    val wherePredicate = (builder.where, where) match {
      case (p, None)                  => p
      case (commands.True(), Some(w)) => w.toLegacyPredicate
      case (p, Some(w))               => commands.And(p, w.toLegacyPredicate)
    }

    builder.startItems(builder.startItems ++ startItems: _*).where(wherePredicate)
  }
}

case class Match(optional: Boolean, pattern: Pattern, hints: Seq[Hint], where: Option[Where])(val token: InputToken) extends Clause with SemanticChecking {
  def name = "MATCH"

  def semanticCheck =
      pattern.semanticCheck(Pattern.SemanticContext.Match) then
      hints.semanticCheck then
      where.semanticCheck

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val matches = builder.matching ++ pattern.toLegacyPatterns
    val namedPaths = builder.namedPaths ++ pattern.toLegacyNamedPaths
    val indexHints = builder.using ++ hints.map(_.toLegacySchemaIndex)
    val wherePredicate = (builder.where, where) match {
      case (p, None)                  => p
      case (commands.True(), Some(w)) => w.toLegacyPredicate
      case (p, Some(w))               => commands.And(p, w.toLegacyPredicate)
    }

    builder.
      matches(matches: _*).
      namedPaths(namedPaths: _*).
      using(indexHints: _*).
      where(wherePredicate).
      isOptional(optional)
  }
}

case class Merge(pattern: Pattern, actions: Seq[MergeAction])(val token: InputToken) extends UpdateClause {
  def name = "MERGE"

  def semanticCheck =
    pattern.semanticCheck(Pattern.SemanticContext.Merge) then
    actions.semanticCheck

  def legacyUpdateActions = toCommand.nextStep()
  def toCommand = {
    val toAbstractPatterns = pattern.toAbstractPatterns
    val map = actions.map(_.toAction)
    val legacyPatterns = pattern.toLegacyPatterns.filterNot(_.isInstanceOf[commands.SingleNode])
    val creates = pattern.toLegacyCreates.filterNot(_.isInstanceOf[CreateNode])
    commands.MergeAst(toAbstractPatterns, map, legacyPatterns, creates)
  }

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val updates = builder.updates ++ legacyUpdateActions
    val namedPaths = builder.namedPaths ++ pattern.toLegacyNamedPaths
    builder.
      updates(updates: _*).
      namedPaths(namedPaths: _*)
  }
}

case class Create(pattern: Pattern)(val token: InputToken) extends UpdateClause {
  def name = "CREATE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Create)

  lazy val legacyUpdateActions = pattern.toLegacyCreates

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val startItems = builder.startItems ++ legacyUpdateActions.map {
      case createNode: mutation.CreateNode                 => commands.CreateNodeStartItem(createNode)
      case createRelationship: mutation.CreateRelationship => commands.CreateRelationshipStartItem(createRelationship)
    }
    val namedPaths = builder.namedPaths ++ pattern.toLegacyNamedPaths

    builder.startItems(startItems: _*).namedPaths(namedPaths: _*)
  }
}

case class CreateUnique(pattern: Pattern)(val token: InputToken) extends UpdateClause {
  def name = "CREATE UNIQUE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Create)

  def legacyUpdateActions = toCommand.nextStep()._1.map(_.inner)

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val startItems = builder.startItems ++ toCommand.nextStep()._1
    val namedPaths = builder.namedPaths ++ toCommand.nextStep()._2

    builder.startItems(startItems: _*).namedPaths(namedPaths: _*)
  }

  private lazy val toCommand = {
    val abstractPatterns: Seq[AbstractPattern] = pattern.toAbstractPatterns.map(_.makeOutgoing)
    commands.CreateUniqueAst(abstractPatterns)
  }
}

case class SetClause(items: Seq[SetItem])(val token: InputToken) extends UpdateClause {
  def name = "SET"

  def semanticCheck = items.semanticCheck

  def legacyUpdateActions = items.map(_.toLegacyUpdateAction)

  def addToLegacyQuery(b: commands.QueryBuilder) = {
    val updates = b.updates ++ legacyUpdateActions
    b.updates(updates: _*)
  }
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

  def legacyUpdateActions = expressions.map(e => mutation.DeleteEntityAction(e.toCommand))

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val updates = builder.updates ++ legacyUpdateActions
    builder.updates(updates: _*)
  }
}

case class Remove(items: Seq[RemoveItem])(val token: InputToken) extends UpdateClause {
  def name = "REMOVE"

  def semanticCheck = items.semanticCheck

  def legacyUpdateActions = items.map(_.toLegacyUpdateAction)

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val updates = builder.updates ++ legacyUpdateActions
    builder.updates(updates: _*)
  }
}

case class Foreach(identifier: Identifier, expression: Expression, updates: Seq[Clause])(val token: InputToken) extends UpdateClause with SemanticChecking {
  def name = "FOREACH"

  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) then
    expression.expectType(CTCollection(CTAny).covariant) then withScopedState {
      val possibleInnerTypes: TypeGenerator = expression.types(_).unwrapCollections
      identifier.declare(possibleInnerTypes) then updates.semanticCheck
    } then updates.filter(!_.isInstanceOf[UpdateClause]).map(c => SemanticError(s"Invalid use of ${c.name} inside FOREACH", c.token))

  def legacyUpdateActions = Seq(ForeachAction(expression.toCommand, identifier.name, updates.flatMap {
    case update: UpdateClause => update.legacyUpdateActions
    case _                    => throw new ThisShouldNotHappenError("cleishm", "a non update clause in FOREACH didn't fail semantic check")
  }))

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val updateActions = builder.updates ++ legacyUpdateActions
    builder.updates(updateActions: _*)
  }
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

  private def checkAliasedReturnItems: SemanticState => Seq[SemanticError] = state => {
    returnItems match {
      case li: ListedReturnItems => li.items.filter(!_.alias.isDefined).map(i => SemanticError("Expression in WITH must be aliased (use AS)", i.token))
      case _                     => Seq()
    }
  }

  def closeLegacyQueryBuilder(builder: commands.QueryBuilder, close: commands.QueryBuilder => commands.Query): commands.Query = {
    val subBuilder = where.foldLeft(new commands.QueryBuilder())((b, w) => b.where(w.toLegacyPredicate))
    val tailQueryBuilder = builder.tail.fold(subBuilder)(t => subBuilder.tail(t))
    val b = builder.tail(close(tailQueryBuilder))
    super.closeLegacyQueryBuilder(b)
  }

  override def closeLegacyQueryBuilder(builder: commands.QueryBuilder): commands.Query = {
    val builderToClose = where.fold(builder) { w =>
      val subBuilder = new commands.QueryBuilder().where(w.toLegacyPredicate)
      val tailQueryBuilder = builder.tail.fold(subBuilder)(t => subBuilder.tail(t))
      builder.tail(tailQueryBuilder.returns(commands.AllIdentifiers()))
    }
    super.closeLegacyQueryBuilder(builderToClose)
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
