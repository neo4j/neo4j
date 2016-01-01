/**
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
package org.neo4j.cypher.internal.compiler.v2_1.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_1._
import symbols._

trait FilteringExpression extends ScopeIntroducingExpression {
  def name: String
  def expression: Expression
  def innerPredicate: Option[Expression]
  override def arguments = Seq(expression)

  def semanticCheck(ctx: SemanticContext) =
    expression.semanticCheck(ctx) chain
    expression.expectType(CTCollection(CTAny).covariant) chain
    checkInnerPredicate

  protected def checkPredicateDefined =
    when (innerPredicate.isEmpty) {
      SemanticError(s"$name(...) requires a WHERE predicate", position)
    }

  protected def checkPredicateNotDefined =
    when (innerPredicate.isDefined) {
      SemanticError(s"$name(...) should not contain a WHERE predicate", position)
    }

  protected def possibleInnerTypes: TypeGenerator = s =>
    (expression.types(s) constrain CTCollection(CTAny)).unwrapCollections

  private def checkInnerPredicate: SemanticCheck = innerPredicate match {
    case Some(e) => withScopedState {
      identifier.declare(possibleInnerTypes) chain e.semanticCheck(SemanticContext.Simple)
    }
    case None    => SemanticCheckResult.success
  }
}


case class FilterExpression(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression])(val position: InputPosition) extends FilteringExpression {
  val name = "filter"

  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateDefined chain
    super.semanticCheck(ctx) chain
    this.specifyType(expression.types)
}


case class ExtractExpression(
    identifier: Identifier,
    expression: Expression,
    innerPredicate: Option[Expression],
    extractExpression: Option[Expression])(val position: InputPosition) extends FilteringExpression
{
  val name = "extract"

  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateNotDefined chain
    checkExtractExpressionDefined chain
    super.semanticCheck(ctx) chain
    checkInnerExpression

  private def checkExtractExpressionDefined =
    when (extractExpression.isEmpty) {
      SemanticError(s"$name(...) requires '| expression' (an extract expression)", position)
    }

  private def checkInnerExpression: SemanticCheck =
    extractExpression.fold(SemanticCheckResult.success) {
      e => withScopedState {
        identifier.declare(possibleInnerTypes) chain e.semanticCheck(SemanticContext.Simple)
      } chain {
        val outerTypes: TypeGenerator = e.types(_).wrapInCollection
        this.specifyType(outerTypes)
      }
    }
}


case class ListComprehension(
    identifier: Identifier,
    expression: Expression,
    innerPredicate: Option[Expression],
    extractExpression: Option[Expression])(val position: InputPosition) extends FilteringExpression
{
  val name = "[...]"

  override def semanticCheck(ctx: SemanticContext) = super.semanticCheck(ctx) chain checkInnerExpression

  private def checkInnerExpression: SemanticCheck = extractExpression match {
    case Some(e) =>
      withScopedState {
        identifier.declare(possibleInnerTypes) chain e.semanticCheck(SemanticContext.Simple)
      } chain {
        val outerTypes: TypeGenerator = e.types(_).wrapInCollection
        this.specifyType(outerTypes)
      }
    case None    => this.specifyType(expression.types)
  }
}


sealed trait IterablePredicateExpression extends FilteringExpression {
  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateDefined chain
    super.semanticCheck(ctx) chain
    this.specifyType(CTBoolean)
}

case class AllIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression])(val position: InputPosition) extends IterablePredicateExpression {
  val name = "all"
}

case class AnyIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression])(val position: InputPosition) extends IterablePredicateExpression {
  val name = "any"
}

case class NoneIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression])(val position: InputPosition) extends IterablePredicateExpression {
  val name = "none"
}

case class SingleIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression])(val position: InputPosition) extends IterablePredicateExpression {
  val name = "single"
}


object ReduceExpression {
  val AccumulatorExpressionTypeMismatchMessageGenerator = (expected: String, existing: String) => s"accumulator is $expected but expression has type $existing"
}

case class ReduceExpression(accumulator: Identifier, init: Expression, identifier: Identifier, collection: Expression, expression: Expression)(val position: InputPosition) extends ScopeIntroducingExpression {
  import ReduceExpression._

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    init.semanticCheck(ctx) chain
    collection.semanticCheck(ctx) chain
    collection.expectType(CTCollection(CTAny).covariant) chain
    withScopedState {
      val indexType: TypeGenerator = s =>
        (collection.types(s) constrain CTCollection(CTAny)).unwrapCollections
      val accType: TypeGenerator = init.types

      identifier.declare(indexType) chain
      accumulator.declare(accType) chain
      expression.semanticCheck(SemanticContext.Simple)
    } chain expression.expectType(init.types, AccumulatorExpressionTypeMismatchMessageGenerator) chain
    this.specifyType(s => init.types(s) leastUpperBounds expression.types(s))
}
