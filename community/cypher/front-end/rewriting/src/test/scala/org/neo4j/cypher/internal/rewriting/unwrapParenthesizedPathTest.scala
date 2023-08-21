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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.rewriting.rewriters.unwrapParenthesizedPath
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class unwrapParenthesizedPathTest extends CypherFunSuite with RewriteTest {

  override def rewriterUnderTest: Rewriter = unwrapParenthesizedPath.instance

  test("Unwrap in concatenation") {
    assertRewrite(
      "MATCH ((a)-->(b)) ((x)-->(y))* RETURN x",
      "MATCH (a)-->(b) ((x)-->(y))* RETURN x"
    )
    assertRewrite(
      "MATCH ((((a)-->(b)))) ((x)-->(y))* RETURN x",
      "MATCH (a)-->(b) ((x)-->(y))* RETURN x"
    )
    assertRewrite(
      "MATCH ((((a)-->(b)))) (((x)-->(y)))* RETURN x",
      "MATCH (a)-->(b) ((x)-->(y))* RETURN x"
    )
  }

  test("Unwrap a whole parenthesized pattern part") {
    assertRewrite(
      "MATCH ((a)-->(b)-->(c)) RETURN count(*)",
      "MATCH (a)-->(b)-->(c) RETURN count(*)"
    )
  }

  test("Unwrap parenthesized path with predicates if the path selector is ALL") {
    assertRewrite(
      "MATCH ALL ((a:A)-[r:R]->(b) WHERE b.prop > 1) RETURN *",
      "MATCH ALL (a:A)-[r:R]->(b) WHERE b.prop > 1 RETURN *"
    )
  }

  test("Unwraps nested parenthesized paths with predicates") {
    assertRewrite(
      "MATCH ((((((a:A)-[r:R]->(b) WHERE b.prop > 1)) WHERE a.prop > 1))) RETURN *",
      "MATCH (a:A)-[r:R]->(b) WHERE b.prop > 1 AND a.prop > 1 RETURN *"
    )
  }

  test("Unwraps nested parenthesized paths with predicates WITH SHORTEST selector") {
    assertRewrite(
      "MATCH ANY SHORTEST ((((((a:A)-[r:R]->+(b) WHERE b.prop > 1)) WHERE a.prop > 1))) RETURN *",
      "MATCH ANY SHORTEST ((a:A)-[r:R]->+(b) WHERE b.prop > 1 AND a.prop > 1) RETURN *"
    )
  }

  test("Unwraps nested parenthesized paths in QPP") {
    assertRewrite(
      "MATCH () ( ((((a:A)-[r:R]->(b) WHERE b.prop > 1) WHERE a.prop > 1)) )+ () RETURN *",
      "MATCH () ( (a:A)-[r:R]->(b) WHERE b.prop > 1 AND a.prop > 1 )+ () RETURN *"
    )
  }

  test("Does not unwrap parenthesized path with predicates if the path selector is different from ALL") {
    assertIsNotRewritten("MATCH ALL SHORTEST ((a:A)-[r:R]->(b) WHERE b.prop > 1) RETURN *")
  }

  test("Unwraps parenthesized path without predicates if the path selector is different from ALL") {
    assertRewrite(
      "MATCH ALL SHORTEST ((a:A)-[r:R]->+(b)) RETURN *",
      "MATCH ALL SHORTEST (a:A)-[r:R]->+(b) RETURN *"
    )
  }
}
