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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.frontend.v2_3.notification.CartesianProductNotification
import org.neo4j.cypher.internal.frontend.v2_3.symbols._


sealed trait Clause extends ASTNode with ASTPhrase with SemanticCheckable {
  def name: String

  def noteCurrentScope: SemanticCheck = s => SemanticCheckResult.success(s.noteCurrentScope(this))
}

sealed trait UpdateClause extends Clause

case class LoadCSV(withHeaders: Boolean, urlString: Expression, identifier: Identifier, fieldTerminator: Option[StringLiteral])(val position: InputPosition) extends Clause with SemanticChecking {
  val name = "LOAD CSV"

  def semanticCheck: SemanticCheck =
    urlString.semanticCheck(Expression.SemanticContext.Simple) chain
    urlString.expectType(CTString.covariant) chain
    checkFieldTerminator chain
    typeCheck

  private def checkFieldTerminator: SemanticCheck = {
    fieldTerminator match {
      case Some(literal) if literal.value.length != 1 =>
        SemanticError("CSV field terminator can only be one character wide", literal.position)
      case _ => SemanticCheckResult.success
    }
  }

  private def typeCheck: SemanticCheck = {
    val typ = if (withHeaders)
      CTMap
    else
      CTCollection(CTString)

    identifier.declare(typ)
  }
}

case class Start(items: Seq[StartItem], where: Option[Where])(val position: InputPosition) extends Clause {
  val name = "START"

  def semanticCheck = items.semanticCheck chain where.semanticCheck
}

case class Match(optional: Boolean, pattern: Pattern, hints: Seq[UsingHint], where: Option[Where])(val position: InputPosition) extends Clause with SemanticChecking {
  def name = "MATCH"

  def semanticCheck =
    pattern.semanticCheck(Pattern.SemanticContext.Match) chain
    hints.semanticCheck chain
    uniqueHints chain
    where.semanticCheck chain
    checkHints chain
    checkForCartesianProducts chain
    noteCurrentScope

  private def uniqueHints: SemanticCheck = {
    val errors = hints.groupBy(_.identifiers.toSeq).collect {
      case pair@(identifiers, identHints) if identHints.size > 1 =>
        SemanticError("Multiple hints for same identifier are not supported", identifiers.head.position, identHints.map(_.position): _*)
    }.toVector

    (state: SemanticState) => SemanticCheckResult(state, errors)
  }

  private def checkForCartesianProducts: SemanticCheck = (state: SemanticState) => {
    import connectedComponents._
    val cc = connectedComponents(pattern.patternParts)
    //if we have multiple connected components we will have
    //a cartesian product
    val newState = cc.drop(1).foldLeft(state) { (innerState, component) =>
      innerState.addNotification(CartesianProductNotification(position, component.identifiers.map(_.name)))
    }

    SemanticCheckResult(newState, Seq.empty)
  }

  private def checkHints: SemanticCheck = {
    val error: Option[SemanticCheck] = hints.collectFirst {
      case hint@UsingIndexHint(Identifier(identifier), LabelName(labelName), PropertyKeyName(property))
        if !containsLabelPredicate(identifier, labelName)
          || !containsPropertyPredicate(identifier, property) =>
        SemanticError(
          """|Cannot use index hint in this context.
            | Index hints are only supported for the following predicates in WHERE
            | (either directly or as part of a top-level AND):
            | equality comparison, inequality (range) comparison, STARTS WITH,
            | IN condition or checking property existence.
            | The comparison cannot be performed between two property values.
            | Note that the label and property comparison must be specified on a
            | non-optional node""".stripLinesAndMargins, hint.position)
      case hint@UsingScanHint(Identifier(identifier), LabelName(labelName))
        if !containsLabelPredicate(identifier, labelName) =>
        SemanticError(
          """|Cannot use label scan hint in this context.
             | Label scan hints require using a simple label test in WHERE (either directly or as part of a
             | top-level AND). Note that the label must be specified on a non-optional node""".stripLinesAndMargins, hint.position)
      case hint@UsingJoinHint(_)
        if pattern.length == 0 =>
        SemanticError("Cannot use join hint for single node pattern.", hint.position)
    }
    error.getOrElse(SemanticCheckResult.success)
  }

  private def containsPropertyPredicate(identifier: String, property: String): Boolean = {
    val properties: Seq[String] = (where match {
      case Some(w) => w.treeFold(Seq.empty[String]) {
        case Equals(Property(Identifier(id), PropertyKeyName(name)), other) if id == identifier && applicable(other) =>
          (acc, _) => acc :+ name
        case Equals(other, Property(Identifier(id), PropertyKeyName(name))) if id == identifier && applicable(other) =>
          (acc, _) => acc :+ name
        case In(Property(Identifier(id), PropertyKeyName(name)),_) if id == identifier =>
          (acc, _) => acc :+ name
        case predicate@FunctionInvocation(_, _, IndexedSeq(Property(Identifier(id), PropertyKeyName(name))))
          if id == identifier && (predicate.function.contains(functions.Exists) || predicate.function.contains(functions.Has)) =>
          (acc, _) => acc :+ name
        case IsNotNull(Property(Identifier(id), PropertyKeyName(name))) if id == identifier =>
          (acc, _) => acc :+ name
        case StartsWith(Property(Identifier(id), PropertyKeyName(name)), _) if id == identifier =>
          (acc, _) => acc :+ name
        case EndsWith(Property(Identifier(id), PropertyKeyName(name)), _) if id == identifier =>
          (acc, _) => acc :+ name
        case Contains(Property(Identifier(id), PropertyKeyName(name)), _) if id == identifier =>
          (acc, _) => acc :+ name
        case expr: InequalityExpression =>
          (acc, _) => Seq(expr.lhs, expr.rhs).foldLeft(acc) { (acc, expr) =>
            expr match {
              case Property(Identifier(id), PropertyKeyName(name)) if id == identifier =>
                acc :+ name
              case _ => acc
            }
          }
        case _: Where | _: And | _: Ands | _: Set[_] =>
          (acc, children) => children(acc)
        case _ =>
          (acc, _) => acc
      }
      case None => Seq.empty
    }) ++ pattern.treeFold(Seq.empty[String]) {
      case NodePattern(Some(Identifier(id)), _, Some(MapExpression(prop)), _) if identifier == id => {
        case (acc, _) =>
          acc ++ prop.map(_._1.name)
      }
    }
    properties.contains(property)
  }

  /*
   * Checks validity of the other side, X, of expressions such as
   *  USING INDEX ON n:Label(prop) WHERE n.prop = X (or X = n.prop)
   *
   * Returns true if X is a valid expression in this context, otherwise false.
   */
  private def applicable(other: Expression) = {
    other match {
      case _: Property => false
      case f: FunctionInvocation => f.function != Some(functions.Id)
      case _ => true
    }
  }

  private def containsLabelPredicate(identifier: String, label: String): Boolean = {
    var labels = pattern.fold(Seq.empty[String]) {
      case NodePattern(Some(Identifier(id)), labels, _, _) if identifier == id =>
        list => list ++ labels.map(_.name)
    }
    labels = where match {
      case Some(where) => where.treeFold(labels) {
        case HasLabels(Identifier(id), labels) if id == identifier =>
          (acc, _) => acc ++ labels.map(_.name)
        case _: Where | _: And | _: Ands | _: Set[_] =>
          (acc, children) => children(acc)
        case _ =>
          (acc, _) => acc
      }
      case None => labels
    }
    labels.contains(label)
  }
}

case class Merge(pattern: Pattern, actions: Seq[MergeAction])(val position: InputPosition) extends UpdateClause {
  def name = "MERGE"

  def semanticCheck =
    pattern.semanticCheck(Pattern.SemanticContext.Merge) chain
    actions.semanticCheck
}

case class Create(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  def name = "CREATE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Create)
}

case class CreateUnique(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  def name = "CREATE UNIQUE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.CreateUnique)
}

case class SetClause(items: Seq[SetItem])(val position: InputPosition) extends UpdateClause {
  def name = "SET"

  def semanticCheck = items.semanticCheck
}

case class Delete(expressions: Seq[Expression], forced: Boolean)(val position: InputPosition) extends UpdateClause {
  def name = "DELETE"

  def semanticCheck =
    expressions.semanticCheck(Expression.SemanticContext.Simple) chain
    warnAboutDeletingLabels chain
    expressions.expectType(CTNode.covariant | CTRelationship.covariant | CTPath.covariant)

  def warnAboutDeletingLabels =
    expressions.filter(_.isInstanceOf[HasLabels]) map {
      e => SemanticError("DELETE doesn't support removing labels from a node. Try REMOVE.", e.position)
    }
}

case class Remove(items: Seq[RemoveItem])(val position: InputPosition) extends UpdateClause {
  def name = "REMOVE"

  def semanticCheck = items.semanticCheck
}

case class Foreach(identifier: Identifier, expression: Expression, updates: Seq[Clause])(val position: InputPosition) extends UpdateClause with SemanticChecking {
  def name = "FOREACH"

  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) chain
    expression.expectType(CTCollection(CTAny).covariant) chain
    updates.filter(!_.isInstanceOf[UpdateClause]).map(c => SemanticError(s"Invalid use of ${c.name} inside FOREACH", c.position)) ifOkChain
    withScopedState {
      val possibleInnerTypes: TypeGenerator = expression.types(_).unwrapCollections
      identifier.declare(possibleInnerTypes) chain updates.semanticCheck
    }
}

case class Unwind(expression: Expression, identifier: Identifier)(val position: InputPosition) extends Clause {
  def name = "UNWIND"

  override def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Results) chain
      expression.expectType(CTCollection(CTAny).covariant) ifOkChain {
      val possibleInnerTypes: TypeGenerator = expression.types(_).unwrapCollections
      identifier.declare(possibleInnerTypes)
    }
}

sealed trait HorizonClause extends Clause with SemanticChecking {
  def semanticCheck = noteCurrentScope
  def semanticCheckContinuation(previousScope: Scope): SemanticCheck
}

sealed trait ProjectionClause extends HorizonClause with SemanticChecking {
  def distinct: Boolean
  def returnItems: ReturnItems
  def orderBy: Option[OrderBy]
  def skip: Option[Skip]
  def limit: Option[Limit]

  override def semanticCheck =
    super.semanticCheck chain
    returnItems.semanticCheck

  def semanticCheckContinuation(previousScope: Scope): SemanticCheck =  (s: SemanticState) => {
    val specialReturnItems = createSpecialReturnItems(previousScope, s)
    val specialStateForShuffle = specialReturnItems.declareIdentifiers(previousScope)(s).state
    val shuffleResult = (orderBy.semanticCheck chain checkSkip chain checkLimit)(specialStateForShuffle)
    val shuffleErrors = shuffleResult.errors

    // We still need to declare the return items, and register the use of identifiers in the ORDER BY clause. But we
    // don't want to see errors from ORDER BY - we'll get them through shuffleErrors instead
    val orderByResult = (returnItems.declareIdentifiers(previousScope) chain ignoreErrors(orderBy.semanticCheck))(s)
    val fixedOrderByResult =
      if (specialReturnItems.includeExisting) {
        val shuffleScope = shuffleResult.state.currentScope.scope
        val definedHere = specialReturnItems.items.map(_.name).toSet
        orderByResult.copy(orderByResult.state.mergeScope(shuffleScope, definedHere))
      } else
        orderByResult

    SemanticCheckResult(fixedOrderByResult.state, fixedOrderByResult.errors ++ shuffleErrors)
  }

  private def createSpecialReturnItems(previousScope: Scope, s: SemanticState): ReturnItems = {
    // ORDER BY lives in this special scope that has access to things in scope before the RETURN/WITH clause,
    // but also to the identifiers introduced by RETURN/WITH. This is most easily done by turning
    // RETURN a, b, c => RETURN *, a, b, c

    // Except when we are doing DISTINCT or aggregation, in which case we only see the scope introduced by the
    // projecting clause
    val includePreviousScope = !(returnItems.containsAggregate || distinct)
    val specialReturnItems = returnItems.copy(includeExisting = includePreviousScope)(returnItems.position)
    specialReturnItems
  }

  // use an empty state when checking skip & limit, as these have entirely isolated context
  protected def checkSkip: SemanticState => Seq[SemanticError] =
    s => skip.semanticCheck(SemanticState.clean).errors

  protected def checkLimit: SemanticState => Seq[SemanticError] =
    s => limit.semanticCheck(SemanticState.clean).errors

  protected def ignoreErrors(inner: SemanticCheck): SemanticCheck =
    s => SemanticCheckResult.success(inner.apply(s).state)

  def verifyOrderByAggregationUse(fail: (String, InputPosition) => Nothing): Unit = {
    val aggregationInProjection = returnItems.items.map(_.expression).exists(containsAggregate)
    val aggregationInOrderBy = orderBy.exists(_.sortItems.map(_.expression).exists(containsAggregate))
    if (!aggregationInProjection && aggregationInOrderBy)
      fail(s"Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding $name", position)
  }
}

case class With(
    distinct: Boolean,
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    where: Option[Where])(val position: InputPosition) extends ProjectionClause {

  def name = "WITH"

  override def semanticCheck =
    super.semanticCheck chain
    checkAliasedReturnItems

  override def semanticCheckContinuation(previousScope: Scope) =
    super.semanticCheckContinuation(previousScope) chain
    where.semanticCheck

  private def checkAliasedReturnItems: SemanticState => Seq[SemanticError] = state => returnItems match {
    case li: ReturnItems => li.items.filter(!_.alias.isDefined).map(i => SemanticError("Expression in WITH must be aliased (use AS)", i.position))
    case _                     => Seq()
  }
}

case class Return(
    distinct: Boolean,
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    excludedNames: Set[String] = Set.empty)(val position: InputPosition) extends ProjectionClause {

  def name = "RETURN"

  override def semanticCheck = super.semanticCheck chain checkIdentifiersInScope

  protected def checkIdentifiersInScope: SemanticState => Seq[SemanticError] = s =>
    if (returnItems.includeExisting && s.currentScope.isEmpty)
      Seq(SemanticError("RETURN * is not allowed when there are no identifiers in scope", position))
    else
      Seq()
}

case class PragmaWithout(excluded: Seq[Identifier])(val position: InputPosition) extends HorizonClause {
  def name = "_PRAGMA WITHOUT"
  val excludedNames = excluded.map(_.name).toSet

  def semanticCheckContinuation(previousScope: Scope): SemanticCheck = s =>
    SemanticCheckResult.success(s.importScope(previousScope, excludedNames))
}
