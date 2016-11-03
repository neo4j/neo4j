/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.ast

import org.neo4j.cypher.internal.frontend.v3_2._
import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_2.symbols._

trait FilteringExpression extends Expression {
  def name: String
  def variable: Variable
  def expression: Expression
  def innerPredicate: Option[Expression]

  override def arguments = Seq(expression)

  def semanticCheck(ctx: SemanticContext) =
    expression.semanticCheck(ctx) chain
    expression.expectType(CTList(CTAny).covariant) chain
    checkInnerPredicate chain
    failIfAggregrating(innerPredicate)

  protected def failIfAggregrating(e: Expression): Option[SemanticError] =
    if(e.containsAggregate) {
      val message = "Can't use aggregating expressions inside of expressions executing over lists"
      Some(SemanticError(message, expression.position))
    } else None

  protected def failIfAggregrating(in: Option[Expression]): Option[SemanticError] =
    in.flatMap(failIfAggregrating)

  protected def possibleInnerTypes: TypeGenerator = s =>
    (expression.types(s) constrain CTList(CTAny)).unwrapLists

  protected def checkPredicateDefined =
    when (innerPredicate.isEmpty) {
      SemanticError(s"$name(...) requires a WHERE predicate", position)
    }

  protected def checkPredicateNotDefined =
    when (innerPredicate.isDefined) {
      SemanticError(s"$name(...) should not contain a WHERE predicate", position)
    }

  private def checkInnerPredicate: SemanticCheck = innerPredicate match {
    case Some(e) => withScopedState {
      variable.declare(possibleInnerTypes) chain e.semanticCheck(SemanticContext.Simple)
    }
    case None    => SemanticCheckResult.success
  }
}

case class FilterExpression(scope: FilterScope, expression: Expression)(val position: InputPosition) extends FilteringExpression {
  val name = "filter"

  def variable = scope.variable
  def innerPredicate = scope.innerPredicate

  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateDefined chain
    super.semanticCheck(ctx) chain
    this.specifyType(expression.types)
}

object FilterExpression {
  def apply(variable: Variable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): FilterExpression =
    FilterExpression(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class ExtractExpression(scope: ExtractScope, expression: Expression)(val position: InputPosition) extends FilteringExpression
{
  val name = "extract"

  def variable = scope.variable
  def innerPredicate = scope.innerPredicate
  def extractExpression = scope.extractExpression

  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateNotDefined chain
    checkExtractExpressionDefined chain
    super.semanticCheck(ctx) chain
    checkInnerExpression chain
    failIfAggregrating(extractExpression)

  private def checkExtractExpressionDefined =
    when (scope.extractExpression.isEmpty) {
      SemanticError(s"$name(...) requires '| expression' (an extract expression)", position)
    }

  private def checkInnerExpression: SemanticCheck =
    scope.extractExpression.fold(SemanticCheckResult.success) {
      e => withScopedState {
        variable.declare(possibleInnerTypes) chain e.semanticCheck(SemanticContext.Simple)
      } chain {
        val outerTypes: TypeGenerator = e.types(_).wrapInList
        this.specifyType(outerTypes)
      }
    }
}

object ExtractExpression {
  def apply(variable: Variable,
            expression: Expression,
            innerPredicate: Option[Expression],
            extractExpression: Option[Expression])(position: InputPosition): ExtractExpression =
    ExtractExpression(ExtractScope(variable, innerPredicate, extractExpression)(position), expression)(position)
}

case class ListComprehension(scope: ExtractScope, expression: Expression)(val position: InputPosition)
  extends FilteringExpression {

  val name = "[...]"

  def variable = scope.variable
  def innerPredicate = scope.innerPredicate
  def extractExpression = scope.extractExpression

  override def semanticCheck(ctx: SemanticContext) =
    super.semanticCheck(ctx) chain
      checkInnerExpression chain
      failIfAggregrating(extractExpression)

  private def checkInnerExpression: SemanticCheck = extractExpression match {
    case Some(e) =>
      withScopedState {
        variable.declare(possibleInnerTypes) chain e.semanticCheck(SemanticContext.Simple)
      } chain {
        val outerTypes: TypeGenerator = e.types(_).wrapInList
        this.specifyType(outerTypes)
      }
    case None =>
      this.specifyType(expression.types)
  }
}

object ListComprehension {
  def apply(variable: Variable,
            expression: Expression,
            innerPredicate: Option[Expression],
            extractExpression: Option[Expression])(position: InputPosition): ListComprehension =
    ListComprehension(ExtractScope(variable, innerPredicate, extractExpression)(position), expression)(position)
}

case class PatternComprehension(namedPath: Option[Variable], pattern: RelationshipsPattern,
                                predicate: Option[Expression], projection: Expression,
                                outerScope: Set[Variable] = Set.empty)
                               (val position: InputPosition)
  extends ScopeExpression {

  self =>

  def withOuterScope(outerScope: Set[Variable]) =
    copy(outerScope = outerScope)(position)

  override def semanticCheck(ctx: SemanticContext) =
    recordCurrentScope chain
    withScopedState {
      pattern.semanticCheck(Pattern.SemanticContext.Match) chain
      namedPath.map(_.declare(CTPath): SemanticCheck).getOrElse(SemanticCheckResult.success) chain
      predicate.semanticCheck(SemanticContext.Simple) chain
      projection.semanticCheck(SemanticContext.Simple)
    } chain {
      val outerTypes: TypeGenerator = projection.types(_).wrapInList
      self.specifyType(outerTypes)
    }

  override val introducedVariables: Set[Variable] = {
    val introducedInternally = namedPath.toSet ++ pattern.element.allVariables
    val introducedExternally = introducedInternally -- outerScope
    introducedExternally
  }
}

sealed trait IterablePredicateExpression extends FilteringExpression {

  def scope: FilterScope
  def variable = scope.variable
  def innerPredicate = scope.innerPredicate

  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateDefined chain
    super.semanticCheck(ctx) chain
    this.specifyType(CTBoolean)
}

case class AllIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition) extends IterablePredicateExpression {
  val name = "all"
}

object AllIterablePredicate {
  def apply(variable: Variable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): AllIterablePredicate =
    AllIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class AnyIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition) extends IterablePredicateExpression {
  val name = "any"
}

object AnyIterablePredicate {
  def apply(variable: Variable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): AnyIterablePredicate =
    AnyIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class NoneIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition) extends IterablePredicateExpression {
  val name = "none"
}

object NoneIterablePredicate {
  def apply(variable: Variable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): NoneIterablePredicate =
    NoneIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class SingleIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition) extends IterablePredicateExpression {
  val name = "single"
}

object SingleIterablePredicate {
  def apply(variable: Variable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): SingleIterablePredicate =
    SingleIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class ReduceExpression(scope: ReduceScope, init: Expression, list: Expression)(val position: InputPosition) extends Expression {
  import ReduceExpression._

  def variable = scope.variable
  def accumulator = scope.accumulator
  def expression = scope.expression

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    init.semanticCheck(ctx) chain
    list.semanticCheck(ctx) chain
    list.expectType(CTList(CTAny).covariant) chain
    withScopedState {
      val indexType: TypeGenerator = s =>
        (list.types(s) constrain CTList(CTAny)).unwrapLists
      val accType: TypeGenerator = init.types

      variable.declare(indexType) chain
      accumulator.declare(accType) chain
      expression.semanticCheck(SemanticContext.Simple)
    } chain expression.expectType(init.types, AccumulatorExpressionTypeMismatchMessageGenerator) chain
    this.specifyType(s => init.types(s) leastUpperBounds expression.types(s)) chain
    failIfAggregating

  protected def failIfAggregating: Option[SemanticError] =
    if (expression.containsAggregate) {
      val message = "Can't use aggregating expressions inside of expressions executing over lists"
      Some(SemanticError(message, expression.position))
    } else None

}

object ReduceExpression {
  val AccumulatorExpressionTypeMismatchMessageGenerator = (expected: String, existing: String) => s"accumulator is $expected but expression has type $existing"

  def apply(accumulator: Variable, init: Expression, variable: Variable, list: Expression, expression: Expression)(position: InputPosition): ReduceExpression =
    ReduceExpression(ReduceScope(accumulator, variable, expression)(position), init, list)(position)
}

