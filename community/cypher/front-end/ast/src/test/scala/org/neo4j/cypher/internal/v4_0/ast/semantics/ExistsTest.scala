/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.ast.semantics

import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.symbols.CTBoolean

class ExistsTest extends SemanticFunSuite {

  val n: NodePattern = NodePattern(Some(variable("n")), Seq.empty, None)(pos)
  val x: NodePattern = expressions.NodePattern(Some(variable("x")), Seq.empty, None)(pos)
  val r: RelationshipPattern = RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING)(pos)
  val pattern:Pattern = Pattern(Seq(EveryPath(RelationshipChain(n, r, x)(pos))))(pos)
  val property: Property = Property(variable("x"), PropertyKeyName("prop")(pos))(pos)
  val failingProperty: Property = Property(variable("missing"), PropertyKeyName("prop")(pos))(pos)

  test("valid exists subclause passes semantic check") {
    val expression = ExistsSubClause(pattern, Some(property))(pos, Set.empty)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("multiple patterns in inner match should not report error") {
    val multiPattern: Pattern = Pattern(Seq(EveryPath(x), EveryPath(n)))(pos)
    val expression = ExistsSubClause(multiPattern, Some(property))(pos, Set.empty)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("inner where using missing identifier reports error") {
    val expression = ExistsSubClause(pattern, Some(failingProperty))(pos, Set.empty)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("subclause cannot reuse identifier with different type") {
    val expression = ExistsSubClause(pattern, Some(property))(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n defined with conflicting type Boolean (expected Node)", pos, pos)
    )
  }
}
