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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpressionTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should compute dependencies of simple expressions") {
    varFor("a").dependencies should equal(Set(varFor("a")))
    literalInt(1).dependencies should equal(Set())
  }

  test("should compute dependencies of composite expressions") {
    add(varFor("a"), subtract(literalInt(1), varFor("b"))).dependencies should equal(Set(varFor("a"), varFor("b")))
  }

  test("should compute dependencies for filtering expressions") {
    // [x IN (n)-->(k) | head(nodes(x)) ]
    val pat: RelationshipsPattern = RelationshipsPattern(
      RelationshipChain(
        NodePattern(Some(varFor("n")), None, None, None) _,
        RelationshipPattern(None, None, None, None, None, SemanticDirection.OUTGOING) _,
        NodePattern(Some(varFor("k")), None, None, None) _
      ) _
    ) _
    val expr: Expression = listComprehension(
      varFor("x"),
      PatternExpression(pat)(Some(Set(varFor("x"))), Some(Set(varFor("n"), varFor("k")))),
      None,
      Some(function("head", function("nodes", varFor("x"))))
    )

    expr.dependencies should equal(Set(varFor("n"), varFor("k")))
  }

  test("should compute dependencies for nested filtering expressions") {
    // [x IN (n)-->(k) | [y IN [1,2,3] | y] ]
    val pat: RelationshipsPattern = RelationshipsPattern(
      RelationshipChain(
        NodePattern(Some(varFor("n")), None, None, None) _,
        RelationshipPattern(None, None, None, None, None, SemanticDirection.OUTGOING) _,
        NodePattern(Some(varFor("k")), None, None, None) _
      ) _
    ) _
    val innerExpr: Expression = listComprehension(
      varFor("y"),
      listOfInt(1, 2, 3),
      None,
      Some(varFor("y"))
    )
    val expr: Expression = listComprehension(
      varFor("x"),
      PatternExpression(pat)(Some(Set(varFor("x"), varFor("y"))), Some(Set(varFor("n"), varFor("k")))),
      None,
      Some(innerExpr)
    )

    expr.dependencies should equal(Set(varFor("n"), varFor("k")))
  }
}
