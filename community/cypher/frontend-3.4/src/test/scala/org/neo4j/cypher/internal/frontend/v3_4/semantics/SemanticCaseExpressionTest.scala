/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.util.v3_4.DummyPosition
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.v3_4.expressions.{CaseExpression, DummyExpression}

class SemanticCaseExpressionTest extends SemanticFunSuite {

  test("Simple: Should combine types of alternatives") {
    val caseExpression = CaseExpression(
      expression = Some(literal("")),
      alternatives = IndexedSeq(
        (
          literal(""),
          literal(1.0)
        ), (
          literal(""),
          literal(1)
        )
      ),
      default = Some(literal(1.0))
    )(DummyPosition(2))

    val result = SemanticExpressionCheck.simple(caseExpression)(SemanticState.clean)
    result.errors shouldBe empty
    types(caseExpression)(result.state) should equal(CTInteger | CTFloat)
  }

  test("Generic: Should combine types of alternatives") {
    val caseExpression = CaseExpression(
      None,
      IndexedSeq(
        (
          DummyExpression(CTBoolean),
          DummyExpression(CTFloat | CTString)
        ), (
          DummyExpression(CTBoolean),
          DummyExpression(CTInteger)
        )
      ),
      Some(DummyExpression(CTFloat | CTNode))
    )(DummyPosition(2))

    val result = SemanticExpressionCheck.simple(caseExpression)(SemanticState.clean)
    result.errors shouldBe empty
    types(caseExpression)(result.state) should equal(CTInteger | CTFloat | CTString | CTNode)
  }

  test("Generic: should type check predicates") {
    val caseExpression = CaseExpression(
      None,
      IndexedSeq(
        (
          DummyExpression(CTBoolean),
          DummyExpression(CTFloat)
        ), (
          DummyExpression(CTString, DummyPosition(12)),
          DummyExpression(CTInteger)
        )
      ),
      Some(DummyExpression(CTFloat))
    )(DummyPosition(2))

    val result = SemanticExpressionCheck.simple(caseExpression)(SemanticState.clean)
    result.errors should have size 1
    result.errors.head.msg should equal("Type mismatch: expected Boolean but was String")
    result.errors.head.position should equal(DummyPosition(12))
  }
}
