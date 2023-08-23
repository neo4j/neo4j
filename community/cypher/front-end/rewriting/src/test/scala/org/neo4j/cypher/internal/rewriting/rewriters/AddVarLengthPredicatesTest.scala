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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AddVarLengthPredicatesTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {

  test("should add predicates for simple var-length") {
    assertRewrite(
      "MATCH (a)-[r1:T*0..2]->(b) RETURN *",
      "MATCH (a)-[r1:T*0..2]->(b) WHERE size(r1) >= 0 AND size(r1) <= 2 RETURN *"
    )
  }

  test("should add lower bound predicates for unbounded var-length") {
    assertRewrite(
      "MATCH (a)-[r1*]->(b) RETURN *",
      "MATCH (a)-[r1*]->(b) WHERE size(r1) >= 1 RETURN *"
    )
  }

  test("should not add predicates in shortest path") {
    assertIsNotRewritten(
      "MATCH (a)-[r1]->(b)-[r2]->(c), allShortestPaths((a)-[r*]->(b)) RETURN *"
    )
  }

  test("should add predicates in shortest path") {
    assertRewrite(
      "MATCH SHORTEST 1 (a)-[r1*1..5]->(b)-[r2]->(c) RETURN *",
      "MATCH SHORTEST 1 ((a)-[r1*1..5]->(b)-[r2]->(c) WHERE size(r1) >= 1 AND size(r1) <= 5) RETURN *"
    )
  }

  test("should add predicates in EXISTS clause") {
    assertRewrite(
      """MATCH (a), (b)
        |WHERE EXISTS { (a)-[r*]->(b) }
        |RETURN *""".stripMargin,
      """MATCH (a), (b)
        |WHERE EXISTS { (a)-[r*]->(b) WHERE size(r) >= 1 }
        |RETURN *""".stripMargin
    )
  }

  def rewriterUnderTest: Rewriter = inSequence(
    nameAllPatternElements(new AnonymousVariableNameGenerator),
    AddVarLengthPredicates.rewriter,
    VarLengthRewriter
  )
}
