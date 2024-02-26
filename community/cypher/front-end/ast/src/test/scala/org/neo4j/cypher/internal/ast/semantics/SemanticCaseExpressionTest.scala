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
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.DummyExpression
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTString

class SemanticCaseExpressionTest extends SemanticFunSuite {

  test("Simple: Should combine types of alternatives") {
    val caseExpression = CaseExpression(
      expression = Some(literal("")),
      alternatives = IndexedSeq(
        (
          equals(literal(""), literal("")),
          literal(1.0)
        ),
        (
          equals(literal(""), literal("")),
          literal(1)
        )
      ),
      default = Some(literal(1.0))
    )(DummyPosition(2))

    val result = SemanticExpressionCheck.simple(caseExpression).run(SemanticState.clean)
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
        ),
        (
          DummyExpression(CTBoolean),
          DummyExpression(CTInteger)
        )
      ),
      Some(DummyExpression(CTFloat | CTNode))
    )(DummyPosition(2))

    val result = SemanticExpressionCheck.simple(caseExpression).run(SemanticState.clean)
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
        ),
        (
          DummyExpression(CTString, DummyPosition(12)),
          DummyExpression(CTInteger)
        )
      ),
      Some(DummyExpression(CTFloat))
    )(DummyPosition(2))

    val result = SemanticExpressionCheck.simple(caseExpression).run(SemanticState.clean)
    result.errors should have size 1
    result.errors.head.msg should equal("Type mismatch: expected Boolean but was String")
    result.errors.head.position should equal(DummyPosition(12))
  }
}
