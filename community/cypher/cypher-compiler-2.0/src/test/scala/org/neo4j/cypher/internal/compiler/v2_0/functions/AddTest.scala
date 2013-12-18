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
import symbols._
import org.junit.Test
import org.scalatest.Assertions

class AddTest extends Assertions {

  @Test
  def shouldHaveNumberTypeWhenAddingNumbers() {
    testValidTypes(
      TypeSet(IntegerType()),
      TypeSet(IntegerType()),
      TypeSet(IntegerType()))

    testValidTypes(
      TypeSet(DoubleType()),
      TypeSet(DoubleType()),
      TypeSet(DoubleType()))

    testValidTypes(
      TypeSet(DoubleType()),
      TypeSet(IntegerType()),
      TypeSet(DoubleType()))

    testValidTypes(
      TypeSet(IntegerType()),
      TypeSet(DoubleType()),
      TypeSet(DoubleType()))

    testValidTypes(
      TypeSet(StringType(), IntegerType()),
      TypeSet(IntegerType()),
      TypeSet(StringType(), IntegerType()))
  }

  @Test
  def shouldHaveStringTypeWhenAddingStrings() {
    testValidTypes(
      TypeSet(StringType()),
      TypeSet(StringType()),
      TypeSet(StringType()))
  }

  @Test
  def shouldHaveStringTypeWhenAddingStringAndNumber() {
    testValidTypes(
      TypeSet(StringType()),
      TypeSet(IntegerType()),
      TypeSet(StringType()))

    testValidTypes(
      TypeSet(StringType()),
      TypeSet(StringType(), IntegerType()),
      TypeSet(StringType()))

    testValidTypes(
      TypeSet(StringType()),
      TypeSet(StringType(), DoubleType()),
      TypeSet(StringType()))

    testValidTypes(
      TypeSet(StringType(), IntegerType()),
      TypeSet(StringType()),
      TypeSet(StringType()))

    testValidTypes(
      TypeSet(StringType(), DoubleType()),
      TypeSet(StringType(), IntegerType()),
      TypeSet(StringType(), DoubleType()))

    testValidTypes(
      TypeSet(StringType(), IntegerType()),
      TypeSet(StringType(), DoubleType()),
      TypeSet(StringType(), DoubleType()))
  }

  @Test
  def shouldHaveCollectionTypeWhenAddingCollection() {
    testValidTypes(
      TypeSet(CollectionType(StringType())),
      TypeSet(CollectionType(StringType())),
      TypeSet(CollectionType(StringType())))

    testValidTypes(
      TypeSet(CollectionType(AnyType())),
      TypeSet(CollectionType(AnyType())),
      TypeSet(CollectionType(AnyType())))

    testValidTypes(
      TypeSet(CollectionType(IntegerType())),
      TypeSet(CollectionType(StringType()), CollectionType(IntegerType())),
      TypeSet(CollectionType(IntegerType())))

    testValidTypes(
      TypeSet(CollectionType(StringType()), CollectionType(IntegerType())),
      TypeSet(CollectionType(IntegerType())),
      TypeSet(CollectionType(IntegerType())))
  }

  @Test
  def shouldHaveCollectionTypeWhenAddingInnerToCollection() {
    testValidTypes(
      TypeSet(CollectionType(StringType())),
      TypeSet(StringType()),
      TypeSet(CollectionType(StringType())))

    testValidTypes(
      TypeSet(CollectionType(StringType())),
      TypeSet(StringType(), IntegerType()),
      TypeSet(CollectionType(StringType())))

    testValidTypes(
      TypeSet(CollectionType(StringType()), CollectionType(IntegerType())),
      TypeSet(StringType()),
      TypeSet(CollectionType(StringType())))
  }

  @Test
  def shouldFailTypeCheckWhenAddingIncompatibleScalars() {
    testInvalidTypes(
      TypeSet(IntegerType()),
      TypeSet(StringType()),
      "Type mismatch: expected Double, Integer, Long or Number but was String"
    )

    testInvalidTypes(
      TypeSet(IntegerType()),
      TypeSet(BooleanType()),
      "Type mismatch: expected Double, Integer, Long or Number but was Boolean"
    )
  }

  @Test
  def shouldFailTypeCheckWhenAddingIncompatibleScalarToCollection() {
    testInvalidTypes(
      TypeSet(CollectionType(IntegerType())),
      TypeSet(StringType()),
      "Type mismatch: expected Integer or Collection<Integer> but was String"
    )
  }

  @Test
  def shouldFailTypeCheckWhenAddingIncompatibleCollections() {
    testInvalidTypes(
      TypeSet(CollectionType(IntegerType())),
      TypeSet(CollectionType(StringType())),
      "Type mismatch: expected Integer or Collection<Integer> but was Collection<String>"
    )
  }

  private def testValidTypes(lhsType: TypeSet, rhsType: TypeSet, expected: TypeSet) {
    val (result, invocation) = evaluateWithTypes(lhsType, rhsType)
    assert(result.errors === Seq())
    assert(invocation.types(result.state) === expected)
  }

  private def testInvalidTypes(lhsType: TypeSet, rhsType: TypeSet, message: String) {
    val (result, _) = evaluateWithTypes(lhsType, rhsType)
    assert(result.errors.length === 1)
    assert(result.errors.head.msg.lines.next().trim === message)
  }

  private def evaluateWithTypes(lhsType: TypeSet, rhsType: TypeSet): (SemanticCheckResult, ast.FunctionInvocation) = {
    val lhs = ast.Identifier("n", DummyToken(11, 12))
    val rhs = ast.Identifier("m", DummyToken(13, 14))
    val invocation = ast.FunctionInvocation(
      ast.Identifier("+", DummyToken(6, 7)),
      false,
      Seq(lhs, rhs),
      DummyToken(5,14)
    )

    val state = (
      lhs.declare(lhsType) then rhs.declare(rhsType)
    )(SemanticState.clean).state

    (invocation.semanticCheck(ast.Expression.SemanticContext.Simple)(state), invocation)
  }

}
