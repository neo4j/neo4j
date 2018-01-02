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

import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticError, SemanticState}

class ContainerIndexTest extends CypherFunSuite {

  val dummyString = DummyExpression(CTString)
  val dummyInteger = DummyExpression(CTInteger)
  val dummyNode = DummyExpression(CTNode)
  val dummyAny = DummyExpression(CTAny)
  val dummyCollection = DummyExpression(CTCollection(CTNode) | CTCollection(CTString))

  test("should detect collection lookup") {
    val lhs = dummyCollection
    val rhs = dummyInteger
    val index = ContainerIndex(lhs, rhs)(DummyPosition(10))

    val result = index.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    assertIsCollection(lhs.types(result.state))
    rhs.types(result.state) should equal(CTInteger.covariant)
    index.types(result.state) should equal(CTNode | CTString)
  }

  test("should detect node lookup") {
    val lhs = dummyNode
    val rhs = dummyString
    val index = ContainerIndex(lhs, rhs)(DummyPosition(10))

    val result = index.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    assertIsMap(lhs.types(result.state))
    rhs.types(result.state) should equal(CTString.covariant)
    index.types(result.state) should equal(CTAny.covariant)
  }

  test("should type as any if given untyped lookup arguments") {
    val lhs = dummyAny
    val rhs = dummyAny
    val index = ContainerIndex(lhs, rhs)(DummyPosition(10))

    val result = index.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    lhs.types(result.state) should equal(CTAny.contravariant)
    rhs.types(result.state) should equal(CTAny.contravariant)
    index.types(result.state) should equal(TypeSpec.all)
  }

  test("should return collection inner types of expression") {
    val index = ContainerIndex(dummyCollection,
      SignedDecimalIntegerLiteral("1")(DummyPosition(5))
    )(DummyPosition(4))

    val result = index.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    index.types(result.state) should equal(CTNode | CTString)
  }

  test("should raise error if indexing by fraction") {
    val index = ContainerIndex(dummyCollection,
      DecimalDoubleLiteral("1.3")(DummyPosition(5))
    )(DummyPosition(4))

    val result = index.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("Type mismatch: expected Integer but was Float", index.idx.position)))
  }

  private def assertIsCollection(spec: TypeSpec) = {
    val intersection = spec & CTCollection(CTAny).covariant
    (intersection == TypeSpec.none) should be(right = false)
    spec should equal(intersection)
  }

  private def assertIsMap(spec: TypeSpec) = {
    val intersection = spec & CTMap.covariant
    (intersection == TypeSpec.none) should be(right = false)
    spec should equal(intersection)
  }
}
