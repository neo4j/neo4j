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

import Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.ast
import org.neo4j.cypher.internal.frontend.v2_3.{TypeGenerator, InputPosition}
import org.neo4j.cypher.internal.frontend.v2_3.symbols.{CypherType, TypeSpec, _}

case class Add(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression {
  def semanticCheck(ctx: SemanticContext) =
    lhs.semanticCheck(ctx) chain
    lhs.expectType(TypeSpec.all) chain
    rhs.semanticCheck(ctx) chain
    rhs.expectType(infixRhsTypes(lhs)) chain
    specifyType(infixOutputTypes(lhs, rhs))

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
    val collectionTypes = lhsTypes constrain CTCollection(CTAny)

    // [a] + b => [a, b]
    val lhsCollectionTypes = collectionTypes | collectionTypes.unwrapCollections

    // a + [b] => [a, b]
    val rhsCollectionTypes = lhsTypes.wrapInCollection

    valueTypes | lhsCollectionTypes | rhsCollectionTypes
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

    val collectionTypes = {
      val lhsCollectionTypes = lhsTypes constrain CTCollection(CTAny)
      val rhsCollectionTypes = rhsTypes constrain CTCollection(CTAny)
      val lhsCollectionInnerTypes = lhsCollectionTypes.unwrapCollections
      val rhsCollectionInnerTypes = rhsCollectionTypes.unwrapCollections

      // [a] + [b] => [a, b]
      (lhsCollectionTypes intersect rhsCollectionTypes) |
        // [a] + b => [a, b]
        (rhsTypes intersectOrCoerce lhsCollectionInnerTypes).wrapInCollection |
        // a + [b] => [a, b]
        (lhsTypes intersectOrCoerce rhsCollectionInnerTypes).wrapInCollection
    }

    stringTypes | numberTypes | collectionTypes
  }

  override def canonicalOperatorSymbol = "+"
}

case class UnaryAdd(rhs: Expression)(val position: InputPosition)
  extends Expression with LeftUnaryOperatorExpression with PrefixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger), outputType = CTInteger),
    Signature(argumentTypes = Vector(CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "+"
}

case class Subtract(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    Signature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "-"
}

case class UnarySubtract(rhs: Expression)(val position: InputPosition)
  extends Expression with LeftUnaryOperatorExpression with PrefixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger), outputType = CTInteger),
    Signature(argumentTypes = Vector(CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "-"
}

case class Multiply(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  // 1 * 1 => 1
  // 1 * 1.1 => 1.1
  // 1.1 * 1 => 1.1
  // 1.1 * 1.1 => 1.21
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    Signature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "*"
}

case class Divide(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  // 1 / 1 => 1
  // 1 / 1.1 => 0.909
  // 1.1 / 1 => 1.1
  // 1.1 / 1.1 => 1.0
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    Signature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "/"
}

case class Modulo(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  // 1 % 1 => 0
  // 1 % 1.1 => 1.0
  // 1.1 % 1 => 0.1
  // 1.1 % 1.1 => 0.0
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    Signature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "%"
}

case class Pow(lhs: Expression, rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with InfixFunctionTyping {
  // 1 ^ 1 => 1.1
  // 1 ^ 1.1 => 1.0
  // 1.1 ^ 1 => 1.1
  // 1.1 ^ 1.1 => 1.1105
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "^"
}
