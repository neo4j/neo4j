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
package org.neo4j.cypher.internal.compiler.v2_0.functions

import org.neo4j.cypher.internal.compiler.v2_0._
import ast.convert.ExpressionConverters._
import commands.{expressions => commandexpressions}
import symbols._

case object Add extends Function {
  val name = "+"

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    checkMinArgs(invocation, 1) then checkMaxArgs(invocation, 2) then
    when(invocation.arguments.length == 1) {
      semanticCheckUnary(ctx, invocation)
    } then when(invocation.arguments.length == 2) {
      semanticCheckInfix(ctx, invocation)
    }

  private def semanticCheckUnary(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    invocation.arguments.expectType(CTInteger.covariant | CTDouble.covariant) then
    invocation.specifyType(invocation.arguments(0).types)

  private def semanticCheckInfix(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck = {
    val lhs = invocation.arguments(0)
    val rhs = invocation.arguments(1)

    lhs.expectType(TypeSpec.all) then
    rhs.expectType(infixRhsTypes(lhs)) then
    invocation.specifyType(infixOutputTypes(lhs, rhs))
  }

  private def infixRhsTypes(lhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = lhs.types(s)

    // "a" + "b" => "ab"
    // "a" + 1 => "a1"
    // "a" + 1.1 => "a1.1"
    val stringTypes =
      if (lhsTypes containsAny CTString.covariant)
        CTString.covariant | CTInteger.covariant | CTDouble.covariant
      else
        TypeSpec.none

    // 1 + "b" => "1b"
    // 1 + 1 => 2
    // 1 + 1.1 => 2.1
    // 1.1 + "b" => "1.1b"
    // 1.1 + 1 => 2.1
    // 1.1 + 1.1 => 2.2
    val numberTypes =
      if (lhsTypes containsAny (CTInteger.covariant | CTDouble.covariant))
        CTString.covariant | CTInteger.covariant | CTDouble.covariant
      else
        TypeSpec.none

    // [a] + [b] => [a, b]
    // [a] + b => [a, b]
    val collectionTypes = (lhsTypes constrain CTCollection(CTAny)) | (lhsTypes constrain CTCollection(CTAny)).unwrapCollections

    // a + [b] => [a, b]
    val rhsCollectionTypes = lhsTypes.wrapInCollection

    stringTypes | numberTypes | collectionTypes | rhsCollectionTypes
  }

  def infixOutputTypes(lhs: ast.Expression, rhs: ast.Expression): TypeGenerator = s => {
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
      when(CTString.covariant, CTInteger.covariant | CTDouble.covariant | CTString.covariant)(CTString)

    // 1 + 1 => 2
    // 1 + 1.1 => 2.1
    // 1.1 + 1 => 2.1
    // 1.1 + 1.1 => 2.2
    val numberTypes: TypeSpec =
      when(CTInteger.covariant, CTInteger.covariant)(CTInteger) |
      when(CTDouble.covariant, CTDouble.covariant | CTInteger.covariant)(CTDouble)

    // [a] + [b] => [a, b]
    // [a] + b => [a, b]
    // a + [b] => [a, b]
    val collectionTypes = {
      val lhsCollectionTypes = lhsTypes constrain CTCollection(CTAny)
      val rhsCollectionTypes = rhsTypes constrain CTCollection(CTAny)
      val lhsCollectionInnerTypes = lhsCollectionTypes.unwrapCollections
      val rhsCollectionInnerTypes = rhsCollectionTypes.unwrapCollections

      (lhsCollectionTypes intersect rhsCollectionTypes) |
      (rhsTypes intersectOrCoerce lhsCollectionInnerTypes).wrapInCollection |
      (lhsTypes intersectOrCoerce rhsCollectionInnerTypes).wrapInCollection
    }

    stringTypes | numberTypes | collectionTypes
  }

  def asCommandExpression(invocation: ast.FunctionInvocation) = {
    if (invocation.arguments.length == 1) {
      invocation.arguments(0).asCommandExpression
    } else {
      commandexpressions.Add(invocation.arguments(0).asCommandExpression, invocation.arguments(1).asCommandExpression)
    }
  }
}
