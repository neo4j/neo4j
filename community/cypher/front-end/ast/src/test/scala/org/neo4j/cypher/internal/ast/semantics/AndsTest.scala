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
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.DummyExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTBoolean

class AndsTest extends SemanticFunSuite {

  test("should semantic check all expressions in ands") {
    val dummyExpr1 = DummyExpression(CTBoolean, DummyPosition(1))
    val dummyExpr2 = DummyExpression(CTBoolean, DummyPosition(2))
    val dummyExpr3 = DummyExpression(CTBoolean, DummyPosition(3))
    val ands = Ands(ListSet[Expression](dummyExpr1, dummyExpr2, dummyExpr3))(pos)
    val result = SemanticExpressionCheck.simple(ands).run(SemanticState.clean)

    result.errors shouldBe empty
    (result.state.typeTable.keySet.map(_.node) should contain).allOf(dummyExpr1, dummyExpr2, dummyExpr3, ands)
  }
}
