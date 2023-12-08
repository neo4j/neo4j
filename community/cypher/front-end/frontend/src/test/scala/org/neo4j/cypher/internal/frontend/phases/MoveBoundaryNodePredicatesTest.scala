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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class MoveBoundaryNodePredicatesTest extends CypherFunSuite
    with RewritePhaseTest
    with TestName
    with AstConstructionTestSupport {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] = MoveBoundaryNodePredicates

  override def semanticFeatures: Seq[SemanticFeature] = Seq(SemanticFeature.GpmShortestPath, SemanticFeature.MatchModes)

  override def preProcessPhase(features: SemanticFeature*): Transformer[BaseContext, BaseState, BaseState] =
    super.preProcessPhase(features: _*) andThen
      flattenBooleanOperators

  test("MATCH (start:A) (()-->())+ () RETURN count(*) AS c") {
    assertNotRewritten(testName)
  }

  test("MATCH (start:A) (()-->())+ (), (start2:A) (()-->())+ () RETURN count(*) AS c") {
    assertNotRewritten(testName)
  }

  test("MATCH ANY SHORTEST (start) (()-->())+ () RETURN count(*) AS c") {
    assertNotRewritten(testName)
  }

  test("MATCH ANY SHORTEST ((start) (()-->())+ (middle:M)--() WHERE start.prop = middle.prop) RETURN count(*) AS c") {
    assertNotRewritten(testName)
  }

  test("MATCH ANY SHORTEST (start:A) (()-->())+ () RETURN count(*) AS c") {
    assertRewritten(testName, "MATCH ANY SHORTEST ((start) (()-->())+ ()) WHERE start:A RETURN count(*) AS c")
  }

  test("MATCH ANY SHORTEST (start) (()-->())+ (end:B) RETURN count(*) AS c") {
    assertRewritten(testName, "MATCH ANY SHORTEST ((start) (()-->())+ (end)) WHERE end:B RETURN count(*) AS c")
  }

  test("MATCH ANY SHORTEST ((start) (()-->())+ (end) WHERE start.prop = end.prop) RETURN count(*) AS c") {
    assertRewritten(
      testName,
      "MATCH ANY SHORTEST ((start) (()-->())+ (end)) WHERE start.prop = end.prop RETURN count(*) AS c"
    )
  }

  test("MATCH ANY SHORTEST ((start) (()-->())+ (end) WHERE $param) RETURN count(*) AS c") {
    assertRewritten(testName, "MATCH ANY SHORTEST ((start) (()-->())+ (end)) WHERE $param RETURN count(*) AS c")
  }

  test("MATCH ANY SHORTEST ((start) (()-->())+ (end) WHERE $param > start.prop) RETURN count(*) AS c") {
    assertRewritten(
      testName,
      "MATCH ANY SHORTEST ((start) (()-->())+ (end)) WHERE $param > start.prop RETURN count(*) AS c"
    )
  }

  test("MATCH (a) MATCH ANY SHORTEST ((start) (()-->())+ (end) WHERE a.prop > start.prop) RETURN count(*) AS c") {
    assertRewritten(
      testName,
      "MATCH (a) MATCH ANY SHORTEST ((start) (()-->())+ (end)) WHERE a.prop > start.prop RETURN count(*) AS c"
    )
  }

  test(
    "MATCH REPEATABLE ELEMENTS ANY SHORTEST (start:A) (()-->())+ (), ANY SHORTEST () (()-->())+ (end:B) RETURN count(*) AS c"
  ) {
    assertRewritten(
      testName,
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST (start) (()-->())+ (), ANY SHORTEST () (()-->())+ (end) WHERE start:A AND end:B RETURN count(*) AS c"
    )
  }

  test(
    "MATCH REPEATABLE ELEMENTS ANY SHORTEST (start:A) (()-->())+ (), ANY SHORTEST () (()-->())+ (end:B) WHERE $param RETURN count(*) AS c"
  ) {
    assertRewritten(
      testName,
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST (start) (()-->())+ (), ANY SHORTEST () (()-->())+ (end) WHERE $param AND start:A AND end:B RETURN count(*) AS c"
    )
  }

  test(
    "MATCH REPEATABLE ELEMENTS ANY SHORTEST (start:A) (()-->())+ (), ALL () (()-->()){2, 4} (end:B) RETURN count(*) AS c"
  ) {
    assertRewritten(
      testName,
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST (start) (()-->())+ (), ALL () (()-->()){2, 4} (end:B) WHERE start:A RETURN count(*) AS c"
    )
  }

  test(
    "MATCH REPEATABLE ELEMENTS ANY SHORTEST (start:A) (()-->())+ (), ALL () (()-->()){2, 4} (end:B) WHERE $param RETURN count(*) AS c"
  ) {
    assertRewritten(
      testName,
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST (start) (()-->())+ (), ALL () (()-->()){2, 4} (end:B) WHERE $param AND start:A RETURN count(*) AS c"
    )
  }

  test(
    "MATCH REPEATABLE ELEMENTS ANY SHORTEST (p = (start:A) (()-->())+ ()), ANY SHORTEST () (()-->())+ (end:B) RETURN count(*) AS c"
  ) {
    assertRewritten(
      testName,
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST (p = (start) (()-->())+ ()), ANY SHORTEST () (()-->())+ (end) WHERE start:A AND end:B RETURN count(*) AS c"
    )
  }

}
