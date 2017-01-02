/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_2._
import symbols._

case class And(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Ands(exprs: Set[Expression])(val position: InputPosition) extends Expression with MultiOperatorExpression {

  override def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    (state: SemanticState) =>
      val totalCheck = exprs.foldLeft(SemanticCheckResult.success) { case (check, expr) => check chain expr.semanticCheck(ctx) }
      totalCheck(state)
  }

  override def canonicalOperatorSymbol = "AND"
}

case class Or(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Ors(exprs: Set[Expression])(val position: InputPosition) extends Expression with MultiOperatorExpression {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success

  override def canonicalOperatorSymbol = "OR"
}

case class Xor(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Not(rhs: Expression)(val position: InputPosition) extends Expression with LeftUnaryOperatorExpression with PrefixFunctionTyping {
  val signatures = Vector(
    Signature(Vector(CTBoolean), outputType = CTBoolean)
  )
}

case class Equals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "="
}

case class NotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "<>"
}

case class InvalidNotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    SemanticError("Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing)", position)

  override def canonicalOperatorSymbol = "!="
}

case class RegexMatch(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression  with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "=~"
}

case class In(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression {
  def semanticCheck(ctx: ast.Expression.SemanticContext): SemanticCheck =
    lhs.semanticCheck(ctx) chain
    lhs.expectType(CTAny.covariant) chain
    rhs.semanticCheck(ctx) chain
    rhs.expectType(CTCollection(CTAny).covariant) chain
    specifyType(CTBoolean)
}

case class IsNull(lhs: Expression)(val position: InputPosition) extends Expression with RightUnaryOperatorExpression with PostfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "IS NULL"
}

case class IsNotNull(lhs: Expression)(val position: InputPosition) extends Expression with RightUnaryOperatorExpression with PostfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "IS NOT NULL"
}

case class LessThan(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "<"
}

case class LessThanOrEqual(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = "<="
}

case class GreaterThan(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = ">"
}

case class GreaterThanOrEqual(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = ">="
}
