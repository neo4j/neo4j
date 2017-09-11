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
import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.symbols.{CypherType, TypeSpec, _}
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, SemanticCheckResult, SemanticError, TypeGenerator, ast}

import scala.util.Try

case class Add(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  def semanticCheck(ctx: SemanticContext) =
    lhs.semanticCheck(ctx) chain
    lhs.expectType(TypeSpec.all) chain
    rhs.semanticCheck(ctx) chain
    rhs.expectType(infixRhsTypes(lhs)) chain
    specifyType(infixOutputTypes(lhs, rhs)) chain
    checkBoundary(lhs, rhs)

  private def checkBoundary(lhs: Expression, rhs: Expression): SemanticCheck = (lhs, rhs) match {
    case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.addExact(l.value, r.value)).isFailure =>
      SemanticError(s"result of ${l.value} + ${r.value} cannot be represented as an integer", position)
    case _ => SemanticCheckResult.success
  }

  private def infixRhsTypes(lhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = lhs.types(s)

    // Strings
    // "a" + "b" => "ab"
    // "a" + 1 => "a1"
    // "a" + 1.1 => "a1.1"
    // Numbers
    // 1 + "b" => "1b"
    // 1 + 1 => 2
    // 1 + 1.1 => 2.1
    // 1.1 + "b" => "1.1b"
    // 1.1 + 1 => 2.1
    // 1.1 + 1.1 => 2.2
    val valueTypes =
      if (lhsTypes containsAny (CTInteger.covariant | CTFloat.covariant | CTString.covariant))
        CTString.covariant | CTInteger.covariant | CTFloat.covariant
      else
        TypeSpec.none

    // [a] + [b] => [a, b]
    val listTypes = lhsTypes constrain CTList(CTAny)

    // [a] + b => [a, b]
    val lhsListTypes = listTypes | listTypes.unwrapLists

    // a + [b] => [a, b]
    val rhsListTypes = lhsTypes.wrapInList

    valueTypes | lhsListTypes | rhsListTypes
  }

  private def infixOutputTypes(lhs: ast.Expression, rhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = lhs.types(s)
    val rhsTypes = rhs.types(s)

    def when(fst: TypeSpec, snd: TypeSpec)(result: CypherType): TypeSpec =
      if (lhsTypes.containsAny(fst) && rhsTypes.containsAny(snd) || lhsTypes.containsAny(snd) && rhsTypes.containsAny(fst))
        result.invariant
      else
        TypeSpec.none

    // "a" + "b" => "ab"
    // "a" + 1 => "a1"
    // "a" + 1.1 => "a1.1"
    // 1 + "b" => "1b"
    // 1.1 + "b" => "1.1b"
    val stringTypes: TypeSpec =
      when(CTString.covariant, CTInteger.covariant | CTFloat.covariant | CTString.covariant)(CTString)

    // 1 + 1 => 2
    // 1 + 1.1 => 2.1
    // 1.1 + 1 => 2.1
    // 1.1 + 1.1 => 2.2
    val numberTypes: TypeSpec =
      when(CTInteger.covariant, CTInteger.covariant)(CTInteger) |
        when(CTFloat.covariant, CTFloat.covariant | CTInteger.covariant)(CTFloat)

    val listTypes = {
      val lhsListTypes = lhsTypes constrain CTList(CTAny)
      val rhsListTypes = rhsTypes constrain CTList(CTAny)
      val lhsListInnerTypes = lhsListTypes.unwrapLists
      val rhsListInnerTypes = rhsListTypes.unwrapLists

      // [a] + [b] => [a, b]
      (lhsListTypes intersect rhsListTypes) |
        // [a] + b => [a, b]
        (rhsTypes intersectOrCoerce lhsListInnerTypes).wrapInList |
        // a + [b] => [a, b]
        (lhsTypes intersectOrCoerce rhsListInnerTypes).wrapInList
    }

    stringTypes | numberTypes | listTypes
  }

  override def canonicalOperatorSymbol = "+"
}

case class UnaryAdd(rhs: Expression)(val position: InputPosition)
  extends Expression with LeftUnaryOperatorExpression with PrefixFunctionTyping {

  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTInteger), outputType = CTInteger),
    ExpressionSignature(argumentTypes = Vector(CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "+"
}

case class Subtract(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {

  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    ExpressionSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    super.semanticCheck(ctx) chain checkBoundary(lhs, rhs)

  private def checkBoundary(lhs: Expression, rhs: Expression): SemanticCheck = (lhs, rhs) match {
    case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.subtractExact(l.value, r.value)).isFailure =>
      SemanticError(s"result of ${l.value} - ${r.value} cannot be represented as an integer", position)
    case _ => SemanticCheckResult.success
  }

  override def canonicalOperatorSymbol = "-"
}

case class UnarySubtract(rhs: Expression)(val position: InputPosition)
  extends Expression with LeftUnaryOperatorExpression with PrefixFunctionTyping {

  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTInteger), outputType = CTInteger),
    ExpressionSignature(argumentTypes = Vector(CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "-"
}

case class Multiply(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {

  // 1 * 1 => 1
  // 1 * 1.1 => 1.1
  // 1.1 * 1 => 1.1
  // 1.1 * 1.1 => 1.21
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    ExpressionSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    super.semanticCheck(ctx) chain checkBoundary(lhs, rhs)

  private def checkBoundary(lhs: Expression, rhs: Expression): SemanticCheck = (lhs, rhs) match {
    case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.multiplyExact(l.value, r.value)).isFailure =>
      SemanticError(s"result of ${l.value} * ${r.value} cannot be represented as an integer", position)
    case _ => SemanticCheckResult.success
  }

  override def canonicalOperatorSymbol = "*"
}

case class Divide(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {

  // 1 / 1 => 1
  // 1 / 1.1 => 0.909
  // 1.1 / 1 => 1.1
  // 1.1 / 1.1 => 1.0
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    ExpressionSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "/"
}

case class Modulo(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {

  // 1 % 1 => 0
  // 1 % 1.1 => 1.0
  // 1.1 % 1 => 0.1
  // 1.1 % 1.1 => 0.0
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    ExpressionSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    ExpressionSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "%"
}

case class Pow(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {

  // 1 ^ 1 => 1.1
  // 1 ^ 1.1 => 1.0
  // 1.1 ^ 1 => 1.1
  // 1.1 ^ 1.1 => 1.1105
  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "^"
}
