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
package org.neo4j.cypher.internal.compiler.v2_0.functions

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.commands.{expressions => commandexpressions}

case object Add extends Function {
  def name = "+"

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) : SemanticCheck =
    checkMinArgs(invocation, 1) then checkMaxArgs(invocation, 2) then
    when(invocation.arguments.length == 1) {
      invocation.arguments.constrainType(NumberType()) then
      invocation.specifyType(invocation.arguments(0).types)
    } then when(invocation.arguments.length == 2) {
      invocation.arguments(0).constrainType(StringType(), NumberType(), CollectionType(AnyType())) then
      invocation.arguments(1).constrainType(validRhsTypes(invocation.arguments(0))) then
      invocation.specifyType(validOutputTypes(invocation.arguments(0), invocation.arguments(1)))
    }

  // TODO: simplify once coercion is available
  private def validRhsTypes(lhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = lhs.types(s)

    val collectionTypes = lhsTypes.constrain(CollectionType(AnyType()))
    val innerTypes = collectionTypes.reparent { case c: CollectionType => c.innerType }

    val lhsStringTypes = lhsTypes.constrain(StringType())
    val stringTypes = if (lhsStringTypes.nonEmpty) lhsStringTypes ++ TypeSet.allNumbers else TypeSet.empty

    val numberTypes = if (lhsTypes.constrain(NumberType()).nonEmpty) TypeSet.allNumbers else TypeSet.empty

    collectionTypes ++ innerTypes ++ stringTypes ++ numberTypes
  }

  // TODO: simplify once coercion is available
  private def validOutputTypes(lhs: ast.Expression, rhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = lhs.types(s)
    val rhsTypes = rhs.types(s)

    val rhsCollectionTypes = rhsTypes.constrain(CollectionType(AnyType())) ++ rhsTypes.reparent(CollectionType(_))
    val collectionTypes = lhsTypes.constrain(CollectionType(AnyType())) constrain rhsCollectionTypes

    val stringTypes = lhsTypes.constrain(StringType())

    val doubleType = if (
      (lhsTypes.contains(DoubleType()) && rhsTypes.constrain(NumberType()).nonEmpty) ||
      (rhsTypes.contains(DoubleType()) && lhsTypes.constrain(NumberType()).nonEmpty)
    )
      TypeSet(DoubleType())
    else
      TypeSet.empty

    val longType = if (
      (lhsTypes.contains(LongType()) && (rhsTypes.contains(LongType()) || rhsTypes.contains(IntegerType()) || rhsTypes.contains(NumberType()))) ||
      (rhsTypes.contains(LongType()) && (lhsTypes.contains(LongType()) || lhsTypes.contains(IntegerType()) || lhsTypes.contains(NumberType())))
    )
      TypeSet(LongType())
    else
      TypeSet.empty

    val integerType = if (
      (lhsTypes.contains(IntegerType()) && (rhsTypes.contains(IntegerType()) || rhsTypes.contains(NumberType()))) ||
      (rhsTypes.contains(IntegerType()) && (lhsTypes.contains(IntegerType()) || lhsTypes.contains(NumberType())))
    )
      TypeSet(IntegerType())
    else
      TypeSet.empty

    val numberType = if (lhsTypes.contains(NumberType()) && rhsTypes.contains(NumberType()))
      TypeSet(NumberType())
    else
      TypeSet.empty

    collectionTypes ++ stringTypes ++ doubleType ++ longType ++ integerType ++ numberType
  }

  def toCommand(invocation: ast.FunctionInvocation) = {
    if (invocation.arguments.length == 1) {
      invocation.arguments(0).toCommand
    } else {
      val left = invocation.arguments(0)
      val right = invocation.arguments(1)
      commandexpressions.Add(left.toCommand, right.toCommand)
    }
  }
}
