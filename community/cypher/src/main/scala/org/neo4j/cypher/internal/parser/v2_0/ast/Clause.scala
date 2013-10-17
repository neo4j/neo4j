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
package org.neo4j.cypher.internal.parser.v2_0.ast

import org.neo4j.cypher.internal.parser.v2_0._
import org.neo4j.cypher.internal.{commands, mutation}
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions}
import org.neo4j.cypher.internal.mutation.{UpdateAction, ForeachAction}
import org.neo4j.cypher.internal.parser.AbstractPattern
import org.neo4j.cypher.internal.symbols._
import org.neo4j.helpers.ThisShouldNotHappenError

sealed trait Clause extends AstNode with SemanticCheckable {
  def name: String
}

sealed trait UpdateClause extends Clause {
  def legacyUpdateActions: Seq[UpdateAction]
}

sealed trait ClosingClause extends Clause {
  def returnItems: ReturnItems
  def orderBy: Option[OrderBy]
  def skip: Option[Skip]
  def limit: Option[Limit]

  def semanticCheck =
    returnItems.semanticCheck then
    checkSortItems then
    checkSkipLimit

  // use a scoped state containing the aliased return items for the sort expressions
  private def checkSortItems : SemanticCheck = s => {
    val result = (returnItems.declareIdentifiers(s) then orderBy.semanticCheck)(s.newScope)
    SemanticCheckResult(result.state.popScope, result.errors)
  }

  // use an empty state when checking skip & limit, as these have isolated scope
  private def checkSkipLimit : SemanticState => Seq[SemanticError] =
    s => (skip ++ limit).semanticCheck(SemanticState.clean).errors

  def closeLegacyQueryBuilder(builder: commands.QueryBuilder) : commands.Query = {
    val returns = returnItems.toCommands
    extractAggregationExpressions(returns).foreach { builder.aggregation(_:_*) }
    skip.foreach { s => builder.skip(s.toCommand) }
    limit.foreach { l => builder.limit(l.toCommand) }
    orderBy.foreach { o => builder.orderBy(o.sortItems.map(_.toCommand):_*) }
    builder.returns(returns:_*)
  }

  protected def extractAggregationExpressions(items: Seq[commands.ReturnColumn]) = {
    val aggregationExpressions = items.collect {
      case commands.ReturnItem(expression, _, _) => (expression.subExpressions :+ expression).collect {
        case agg: commandexpressions.AggregationExpression => agg
      }
    }.flatten

    aggregationExpressions match {
      case Seq() => None
      case _     => Some(aggregationExpressions)
    }
  }
}

trait DistinctClosingClause extends ClosingClause {
  abstract override def extractAggregationExpressions(items: Seq[commands.ReturnColumn]) = {
    Some(super.extractAggregationExpressions(items).getOrElse(Seq()))
  }
}


case class Start(items: Seq[StartItem], where: Option[Where], token: InputToken) extends Clause {
  val name = "START"

  def semanticCheck = items.semanticCheck then where.semanticCheck

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val startItems = items.map(_.toCommand)
    val wherePredicate = (builder.where, where) match {
      case (p, None)                  => p
      case (commands.True(), Some(w)) => w.toLegacyPredicate
      case (p, Some(w))               => commands.And(p, w.toLegacyPredicate)
    }

    builder.startItems((builder.startItems ++ startItems): _*).where(wherePredicate)
  }
}

case class Match(optional: Boolean, pattern: Pattern, hints: Seq[Hint], where: Option[Where], token: InputToken) extends Clause with SemanticChecking {
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

case class Merge(patterns: Seq[PatternPart], actions: Seq[MergeAction], token: InputToken) extends UpdateClause {
  def name = "MERGE"

  def semanticCheck =
    ensureMergeActionIdentifiersNotDeclared then
    patterns.semanticCheck(Pattern.SemanticContext.Update) then
    actions.semanticCheck

  def ensureMergeActionIdentifiersNotDeclared: SemanticState => Seq[SemanticError] = state =>
    actions.filter(a => state.symbol(a.identifier.name).isDefined).map {
      a => SemanticError(s"Invalid use of ${a.identifier.name} for ${a.name}: already defined prior to ${name}", a.identifier.token, a.token, token)
    }

  def legacyUpdateActions = toCommand.nextStep
  def toCommand = commands.MergeAst(patterns.flatMap(_.toAbstractPatterns), actions.map(_.toAction))

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val updates = builder.updates ++ legacyUpdateActions
    builder.updates(updates: _*)
  }
}

case class Create(pattern: Pattern, token: InputToken) extends UpdateClause {
  def name = "CREATE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Update)

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

case class CreateUnique(pattern: Pattern, token: InputToken) extends UpdateClause {
  def name = "CREATE UNIQUE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Update)

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

case class SetClause(items: Seq[SetItem], token: InputToken) extends UpdateClause {
  def name = "SET"

  def semanticCheck = items.semanticCheck

  def legacyUpdateActions = items.map(_.toLegacyUpdateAction)

  def addToLegacyQuery(b: commands.QueryBuilder) = {
    val updates = b.updates ++ legacyUpdateActions
    b.updates(updates: _*)
  }
}

case class Delete(expressions: Seq[Expression], token: InputToken) extends UpdateClause {
  def name = "DELETE"

  def semanticCheck =
    expressions.semanticCheck(Expression.SemanticContext.Simple) then
      warnAboutDeletingLabels then
      expressions.constrainType(NodeType(), RelationshipType(), PathType())

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

case class Remove(items: Seq[RemoveItem], token: InputToken) extends UpdateClause {
  def name = "REMOVE"

  def semanticCheck = items.semanticCheck

  def legacyUpdateActions = items.map(_.toLegacyUpdateAction)

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val updates = builder.updates ++ legacyUpdateActions
    builder.updates(updates: _*)
  }
}

case class Foreach(identifier: Identifier, expression: Expression, updates: Seq[Clause], token: InputToken) extends UpdateClause with SemanticChecking {
  def name = "FOREACH"

  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) then
      expression.constrainType(CollectionType(AnyType())) then withScopedState {
        val innerTypes: TypeGenerator = expression.types(_).map(_.iteratedType)
        identifier.declare(innerTypes) then updates.semanticCheck
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
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    where: Option[Where],
    token: InputToken) extends ClosingClause
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
    builder.tail(close(tailQueryBuilder))
    super.closeLegacyQueryBuilder(builder)
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
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    token: InputToken) extends ClosingClause {
  def name = "RETURN"
}
