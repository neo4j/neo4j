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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.helpers.TestState
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.TestContext
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CollapseMultipleInPredicatesTest extends CypherFunSuite with RewriteTest {

  override def rewriterUnderTest: Rewriter =
    collapseMultipleInPredicates.instance(TestState(None), new TestContext(mock[Monitors]))

  test("should rewrite simple case") {
    assertRewrite(
      "MATCH (n) WHERE n.prop IN [1,2,3] OR n.prop IN [4,5,6] RETURN n.prop",
      "MATCH (n) WHERE n.prop IN [1,2,3,4,5,6] RETURN n.prop"
    )
  }

  test("should rewrite overlapping case") {
    assertRewrite(
      "MATCH (n) WHERE n.prop IN [1,2,3] OR n.prop IN [1,3,5] OR n.prop IN [4,5,6] RETURN n.prop",
      "MATCH (n) WHERE n.prop IN [1,2,3,5,4,6] RETURN n.prop"
    )
  }

  test("should rewrite interleaved case") {
    assertRewrite(
      "MATCH (n) WHERE n.prop IN [1,2,3] OR n.prop2 IN [1,3,5] OR n.prop IN [4,5,6] RETURN n.prop",
      "MATCH (n) WHERE n.prop2 IN [1, 3, 5] OR n.prop IN [1,2,3,4,5,6] RETURN n.prop"
    )
  }

  test("should collapse empty collection and non-empty collection") {
    assertRewrite(
      "MATCH (a) WHERE id(a) IN [] OR id(a) IN [1,2,3] RETURN a",
      "MATCH (a) WHERE id(a) IN [1,2,3] RETURN a"
    )
  }

  test("should collapse empty collection") {
    assertRewrite(
      "MATCH (a) WHERE id(a) IN [] OR a.prop > 1 RETURN a",
      "MATCH (a) WHERE a.prop > 1 RETURN a"
    )
  }

  override protected def parseForRewriting(queryText: String): Statement =
    super.parseForRewriting(queryText).endoRewrite(
      inSequence(flattenBooleanOperators.instance(CancellationChecker.NeverCancelled))
    )
}
