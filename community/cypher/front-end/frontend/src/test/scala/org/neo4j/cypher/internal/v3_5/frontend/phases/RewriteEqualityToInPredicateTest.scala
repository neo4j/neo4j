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
package org.neo4j.cypher.internal.v3_5.frontend.phases

import org.neo4j.cypher.internal.v3_5.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class RewriteEqualityToInPredicateTest extends CypherFunSuite with AstRewritingTestSupport with RewritePhaseTest {

  override def rewriterPhaseUnderTest: Phase[BaseContext, BaseState, BaseState] = rewriteEqualityToInPredicate

  test("MATCH (a) WHERE id(a) = 42 (no dependencies on the RHS)") {
    assertRewritten(
      "MATCH (a) WHERE id(a) = 42 RETURN a",
      "MATCH (a) WHERE id(a) IN [42] RETURN a")
  }

  test("MATCH (a) WHERE a.prop = 42 (no dependencies on the RHS)") {
    assertRewritten(
      "MATCH (a) WHERE a.prop = 42 RETURN a",
      "MATCH (a) WHERE a.prop IN [42] RETURN a")
  }

  test("MATCH (a) WHERE id(a) = rand() (no dependencies on the RHS)") {
    assertRewritten(
      "MATCH (a) WHERE id(a) = rand() RETURN a",
      "MATCH (a) WHERE id(a) IN [rand()] RETURN a")
  }

  test("MATCH (a) WHERE a.prop = rand() (no dependencies on the RHS)") {
    assertRewritten(
      "MATCH (a) WHERE a.prop = rand() RETURN a",
      "MATCH (a) WHERE a.prop IN [rand()] RETURN a")
  }

  test("WITH x as 42 MATCH (a) WHERE id(a) = x (no dependencies on the RHS)") {
    assertRewritten(
      "WITH 42 as x MATCH (a) WHERE id(a) = x RETURN a",
      "WITH 42 as x MATCH (a) WHERE id(a) IN [x] RETURN a")
  }

  test("WITH x as 42 MATCH (a) WHERE a.prop = x (no dependencies on the RHS)") {
    assertRewritten(
      "WITH 42 as x MATCH (a) WHERE a.prop = x RETURN a",
      "WITH 42 as x MATCH (a) WHERE a.prop IN [x] RETURN a")
  }

  test("should not rewrite a comparison between two properties") {
    assertNotRewritten(
      "MATCH (a), (b) WHERE a.prop = b.prop RETURN a")
  }

}
