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

class TransitiveClosureTest extends CypherFunSuite with AstRewritingTestSupport with RewritePhaseTest {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] = transitiveClosure andThen CNFNormalizer
  override def rewriterPhaseForExpected: Transformer[BaseContext, BaseState, BaseState] = CNFNormalizer

  test("MATCH (a)-->(b) WHERE a.prop = b.prop AND b.prop = 42") {
    assertRewritten(
      "MATCH (a)-->(b) WHERE a.prop = b.prop AND b.prop = 42 RETURN a",
      "MATCH (a)-->(b) WHERE a.prop = 42 AND b.prop = 42 RETURN a")
  }

  // pending fix in frontend
  test("MATCH (a)-->(b) WHERE NOT a.prop = b.prop AND b.prop = 42") {
    assertNotRewritten(
      "MATCH (a)-->(b) WHERE NOT a.prop = b.prop AND b.prop = 42 RETURN a")
  }

  // pending fix in frontend
  test("MATCH (a)-->(b) WHERE a.prop = b.prop AND NOT b.prop = 42") {
    assertNotRewritten(
      "MATCH (a)-->(b) WHERE a.prop = b.prop AND NOT b.prop = 42 RETURN a")
  }

  test("MATCH (a)-->(b) WHERE NOT (a.prop = b.prop AND b.prop = 42)") {
    assertRewritten(
      "MATCH (a)-->(b) WHERE NOT (a.prop = b.prop AND b.prop = 42) RETURN a",
      "MATCH (a)-->(b) WHERE NOT (a.prop = 42 AND b.prop = 42) RETURN a")
  }

  test("MATCH (a)-->(b) WHERE b.prop = a.prop AND b.prop = 42") {
    assertRewritten(
      "MATCH (a)-->(b) WHERE b.prop = a.prop AND b.prop = 42 RETURN a",
      "MATCH (a)-->(b) WHERE b.prop = 42 AND a.prop = 42 RETURN a")
  }

  test("MATCH (a)-->(b) WHERE a.prop = b.prop OR b.prop = 42") {
    assertNotRewritten("MATCH (a)-->(b) WHERE a.prop = b.prop OR b.prop = 42 RETURN a")
  }

  test("MATCH (a)-->(b) WHERE a.prop = b.prop AND b.prop = b.prop2 AND b.prop2 = 42") {
    assertRewritten(
      "MATCH (a)-->(b) WHERE a.prop = b.prop AND b.prop = b.prop2 AND b.prop2 = 42 RETURN a",
      "MATCH (a)-->(b) WHERE a.prop = 42 AND b.prop = 42 AND b.prop2 = 42 RETURN a")
  }

  test("MATCH (a)-->(b) WHERE b.prop2 = 42 AND a.prop = b.prop AND b.prop = b.prop2") {
    assertRewritten(
      "MATCH (a)-->(b) WHERE b.prop2 = 42 AND a.prop = b.prop AND b.prop = b.prop2 RETURN a",
      "MATCH (a)-->(b) WHERE b.prop2 = 42 AND a.prop = 42 AND b.prop = 42 RETURN a")
  }

  test("MATCH (a)-->(b) WHERE (a.prop = b.prop AND b.prop = 42) OR (a.prop = b.prop2 AND b.prop2 = 42)") {
    assertRewritten(
      "MATCH (a)-->(b) WHERE (a.prop = b.prop AND b.prop = 42) OR (a.prop = b.prop2 AND b.prop2 = 42) RETURN a",
      "MATCH (a)-->(b) WHERE (a.prop = 42 AND b.prop = 42) OR (a.prop = 42 AND b.prop2 = 42) RETURN a")
  }

  test("MATCH (a)-->(b) WHERE (a.prop = b.prop AND b.prop = 42) OR (a.prop = b.prop AND b.prop2 = 43) OR (a.prop = b.prop AND b.prop2 = 44)") {
    assertRewritten(
      "MATCH (a)-->(b) WHERE (a.prop = b.prop AND b.prop = 42) OR (a.prop = b.prop AND b.prop = 43) OR (a.prop = b.prop AND b.prop = 44) RETURN a",
      "MATCH (a)-->(b) WHERE (a.prop = 42 AND b.prop = 42) OR (a.prop = 43 AND b.prop = 43) OR (a.prop = 44 AND b.prop = 44) RETURN a")
  }
}
