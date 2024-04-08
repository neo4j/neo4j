/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.SemanticCheckInTest.SemanticCheckWithDefaultContext
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DummyExpression
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.TypeSpec

class ContainerIndexTest extends SemanticFunSuite {

  private val dummyString = DummyExpression(CTString)
  private val dummyInteger = DummyExpression(CTInteger)
  private val dummyFloat = DummyExpression(CTFloat)
  private val dummyNode = DummyExpression(CTNode)
  private val dummyRelationship = DummyExpression(CTRelationship)
  private val dummyMap = DummyExpression(CTMap)
  private val dummyAny = DummyExpression(CTAny)
  private val dummyList = DummyExpression(CTList(CTNode) | CTList(CTString))

  test("should detect list lookup") {
    val lhs = dummyList
    val rhs = dummyInteger
    val index = ContainerIndex(lhs, rhs)(DummyPosition(10))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors shouldBe empty
    assertIsList(types(lhs)(result.state))
    types(rhs)(result.state) should equal(CTInteger.covariant)
    types(index)(result.state) should equal(CTNode | CTString)
  }

  test("should detect node lookup") {
    val lhs = dummyNode
    val rhs = dummyString
    val index = ContainerIndex(lhs, rhs)(DummyPosition(10))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors shouldBe empty
    assertIsMap(types(lhs)(result.state))
    types(rhs)(result.state) should equal(CTString.covariant)
    types(index)(result.state) should equal(CTAny.covariant)
  }

  test("should type as any if given untyped lookup arguments") {
    val lhs = dummyAny
    val rhs = dummyAny
    val index = ContainerIndex(lhs, rhs)(DummyPosition(10))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors shouldBe empty
    types(lhs)(result.state) should equal(CTAny.contravariant)
    types(rhs)(result.state) should equal(CTAny.contravariant)
    types(index)(result.state) should equal(TypeSpec.all)
  }

  test("should return list inner types of expression") {
    val index = ContainerIndex(dummyList, SignedDecimalIntegerLiteral("1")(DummyPosition(5)))(DummyPosition(4))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors shouldBe empty
    types(index)(result.state) should equal(CTNode | CTString)
  }

  test("should raise error if indexing by fraction") {
    val index = ContainerIndex(dummyList, DecimalDoubleLiteral("1.3")(DummyPosition(5)))(DummyPosition(4))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors should equal(Seq(SemanticError(
      "Type mismatch: list index must be given as Integer, but was Float",
      index.idx.position
    )))
  }

  test("should raise error if indexing list by string") {
    val index = ContainerIndex(dummyList, StringLiteral("1.3")(DummyPosition(5).withInputLength(2)))(DummyPosition(4))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors should equal(Seq(SemanticError(
      "Type mismatch: list index must be given as Integer, but was String",
      index.idx.position
    )))
  }

  test("should raise error if indexing map by int") {
    val index = ContainerIndex(dummyMap, SignedDecimalIntegerLiteral("1")(DummyPosition(5)))(DummyPosition(4))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors should equal(Seq(SemanticError(
      "Type mismatch: map key must be given as String, but was Integer",
      index.idx.position
    )))
  }

  test("should raise error if indexing node by int") {
    val index = ContainerIndex(dummyNode, SignedDecimalIntegerLiteral("1")(DummyPosition(5)))(DummyPosition(4))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors should equal(Seq(SemanticError(
      "Type mismatch: node or relationship property key must be given as String, but was Integer",
      index.idx.position
    )))
  }

  test("should raise error if indexing relationship by int") {
    val index = ContainerIndex(dummyRelationship, SignedDecimalIntegerLiteral("1")(DummyPosition(5)))(DummyPosition(4))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors should equal(Seq(SemanticError(
      "Type mismatch: node or relationship property key must be given as String, but was Integer",
      index.idx.position
    )))
  }

  test("should raise error if looking up not from a container, with int") {
    val index = ContainerIndex(dummyInteger, dummyInteger)(DummyPosition(10))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors should equal(Seq(SemanticError(
      "Type mismatch: expected List<T> but was Integer",
      index.idx.position
    )))
  }

  test("should raise error if looking up not from a container, with string") {
    val index = ContainerIndex(dummyInteger, dummyString)(DummyPosition(10))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors should equal(Seq(SemanticError(
      "Type mismatch: expected Map, Node or Relationship but was Integer",
      index.idx.position
    )))
  }

  test("should raise error if looking up not from container, with invalid type") {
    val index = ContainerIndex(dummyInteger, dummyFloat)(DummyPosition(10))

    val result = SemanticExpressionCheck.simple(index).run(SemanticState.clean)
    result.errors should equal(Seq(SemanticError(
      "Type mismatch: expected Map, Node, Relationship or List<T> but was Integer",
      index.idx.position
    )))
  }

  private def assertIsList(spec: TypeSpec) = {
    val intersection = spec & CTList(CTAny).covariant
    (intersection == TypeSpec.none) should be(right = false)
    spec should equal(intersection)
  }

  private def assertIsMap(spec: TypeSpec) = {
    val intersection = spec & CTMap.covariant
    (intersection == TypeSpec.none) should be(right = false)
    spec should equal(intersection)
  }
}
