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

import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.CNFNormalizerTest
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CollapseInCollectionsTest extends CypherFunSuite with AstRewritingTestSupport with RewritePhaseTest {

  final private val cnfNormalizerTransformer = CNFNormalizerTest.getTransformer(Nil)

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    cnfNormalizerTransformer andThen collapseMultipleInPredicates

  test("should collapse collection containing ConstValues for id function") {
    assertRewritten(
      "MATCH (a) WHERE id(a) IN [42] OR id(a) IN [13] RETURN a",
      "MATCH (a) WHERE id(a) IN [42, 13] RETURN a"
    )
  }

  test("should collapse collections containing ConstValues and nonConstValues for id function") {
    assertRewritten(
      "MATCH (a) WHERE id(a) IN [42] OR id(a) IN [rand()] RETURN a",
      "MATCH (a) WHERE id(a) IN [42, rand()] RETURN a"
    )
  }

  test("should collapse collection containing ConstValues for property") {
    assertRewritten(
      "MATCH (a) WHERE a.prop IN [42] OR a.prop IN [13] RETURN a",
      "MATCH (a) WHERE a.prop IN [42, 13] RETURN a"
    )
  }

  test("should collapse collections containing ConstValues and nonConstValues for property") {
    assertRewritten(
      "MATCH (a) WHERE a.prop IN [42] OR a.prop IN [rand()] RETURN a",
      "MATCH (a) WHERE a.prop IN [42, rand()] RETURN a"
    )
  }
}
