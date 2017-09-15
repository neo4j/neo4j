/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4.symbols._

case class And(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

object Ands {
  def create(exprs: Set[Expression]): Expression = {
    val size = exprs.size
    if(size == 0)
      True()(InputPosition.NONE)
    else if (size == 1)
      exprs.head
    else
      Ands(exprs)(exprs.head.position)
  }
}

case class Ands(exprs: Set[Expression])(val position: InputPosition) extends Expression with MultiOperatorExpression {

  override def canonicalOperatorSymbol = "AND"
}

case class Or(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Ors(exprs: Set[Expression])(val position: InputPosition) extends Expression with MultiOperatorExpression {
  override def canonicalOperatorSymbol = "OR"
}

case class Xor(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Not(rhs: Expression)(val position: InputPosition) extends Expression with LeftUnaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(Vector(CTBoolean), outputType = CTBoolean)
  )
}

case class Equals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "="

  def switchSides: Equals = copy(rhs, lhs)(position)
}

case class NotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "<>"
}

case class InvalidNotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override def canonicalOperatorSymbol = "!="
}

case class RegexMatch(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "=~"
}

case class In(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression

// Partial predicates are predicates that are covered by a larger predicate which is going to be solved later during planning
// (and then will replace this predicate).
// (i.e. final query graph matches up with original query)
sealed trait PartialPredicate[+P <: Expression] extends Expression {
  def coveredPredicate: P
  def coveringPredicate: Expression
}

object PartialPredicate {

  def apply[P <: Expression](coveredPredicate: P, coveringPredicate: Expression): Expression =
    ifNotEqual(coveredPredicate, coveringPredicate).getOrElse(coveringPredicate)

  def ifNotEqual[P <: Expression](coveredPredicate: P, coveringPredicate: Expression): Option[PartialPredicate[P]] =
    if (coveredPredicate == coveringPredicate) None else Some(PartialPredicateWrapper(coveredPredicate, coveringPredicate))

  final case class PartialPredicateWrapper[P <: Expression](coveredPredicate: P, coveringPredicate: Expression) extends PartialPredicate[P] {
    override def position: InputPosition = coveredPredicate.position
  }
}

case class StartsWith(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )
}

case class EndsWith(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )
}

case class Contains(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )
}

case class IsNull(lhs: Expression)(val position: InputPosition) extends Expression with RightUnaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "IS NULL"
}

case class IsNotNull(lhs: Expression)(val position: InputPosition) extends Expression with RightUnaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "IS NOT NULL"
}

sealed trait InequalityExpression extends Expression with BinaryOperatorExpression {
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTFloat, CTInteger), outputType = CTBoolean),
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTBoolean),
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    ExpressionSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    ExpressionSignature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  def includeEquality: Boolean

  def negated: InequalityExpression
  def swapped: InequalityExpression

  def lhs: Expression
  def rhs: Expression
}

final case class LessThan(lhs: Expression, rhs: Expression)(val position: InputPosition) extends InequalityExpression {
  override val canonicalOperatorSymbol = "<"

  override val includeEquality = false

  override def negated: InequalityExpression = GreaterThanOrEqual(lhs, rhs)(position)
  override def swapped: InequalityExpression = GreaterThan(rhs, lhs)(position)
}

final case class LessThanOrEqual(lhs: Expression, rhs: Expression)(val position: InputPosition) extends InequalityExpression {
  override val canonicalOperatorSymbol = "<="

  override val includeEquality = true

  override def negated: InequalityExpression = GreaterThan(lhs, rhs)(position)
  override def swapped: InequalityExpression = GreaterThanOrEqual(rhs, lhs)(position)
}

final case class GreaterThan(lhs: Expression, rhs: Expression)(val position: InputPosition) extends InequalityExpression {
  override val canonicalOperatorSymbol = ">"

  override val includeEquality = false

  override def negated: InequalityExpression = LessThanOrEqual(lhs, rhs)(position)
  override def swapped: InequalityExpression = LessThan(rhs, lhs)(position)
}

final case class GreaterThanOrEqual(lhs: Expression, rhs: Expression)(val position: InputPosition) extends InequalityExpression {
  override val canonicalOperatorSymbol = ">="

  override val includeEquality = true

  override def negated: InequalityExpression = LessThan(lhs, rhs)(position)
  override def swapped: InequalityExpression = LessThanOrEqual(rhs, lhs)(position)
}
