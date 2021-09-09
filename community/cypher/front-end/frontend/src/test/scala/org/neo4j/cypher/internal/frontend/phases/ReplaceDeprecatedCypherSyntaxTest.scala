/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.rewriting.Deprecations.semanticallyDeprecatedFeaturesIn4_X
import org.neo4j.cypher.internal.rewriting.Deprecations.syntacticallyDeprecatedFeaturesIn4_X
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ReplaceDeprecatedCypherSyntaxTest extends CypherFunSuite with AstConstructionTestSupport with RewritePhaseTest {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    SyntaxDeprecationWarningsAndReplacements(syntacticallyDeprecatedFeaturesIn4_X) andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true) andThen
      SyntaxDeprecationWarningsAndReplacements(semanticallyDeprecatedFeaturesIn4_X)

  override def astRewriteAndAnalyze: Boolean = false

  test("should rewrite timestamp()") {
    assertRewritten(
      "RETURN timestamp() AS t",
      "RETURN datetime().epochMillis AS t"
    )
  }

  test("should also rewrite TiMeStAmP()") {
    assertRewritten(
      "RETURN TiMeStAmP() AS t",
      "RETURN datetime().epochMillis AS t"
    )
  }

  test("should rewrite 0X123 to 0x123") {
    assertRewritten(
      "RETURN 0X123 AS t",
      "RETURN 0x123 AS t"
    )
  }

  test("should rewrite 0X9fff to 0x9fff") {
    assertRewritten(
      "RETURN 0X9fff AS t",
      "RETURN 0x9fff AS t"
    )
  }

  test("should rewrite -0X9FFF to -0x9fff") {
    assertRewritten(
      "RETURN -0X9FFF AS t",
      "RETURN -0x9fff AS t"
    )
  }

  test("should rewrite exists in where") {
    assertRewritten(
      "MATCH (n) WHERE exists(n.prop) RETURN n",
      "MATCH (n) WHERE n.prop IS NOT NULL RETURN n"
    )
    assertRewritten(
      "MATCH (n WHERE exists(n.prop)) RETURN n",
      "MATCH (n WHERE n.prop IS NOT NULL) RETURN n"
    )
  }

  test("should rewrite exists with dynamic property in where") {
    assertRewritten(
      "MATCH (n) WHERE exists(n['prop']) RETURN n",
      "MATCH (n) WHERE n['prop'] IS NOT NULL RETURN n"
    )
    assertRewritten(
      "MATCH (n WHERE exists(n['prop'])) RETURN n",
      "MATCH (n WHERE n['prop'] IS NOT NULL) RETURN n"
    )
  }

  test("should rewrite exists in where nested") {
    assertRewritten(
      "MATCH (n) WHERE exists(n.prop) AND n.foo > 0 RETURN n",
      "MATCH (n) WHERE n.prop IS NOT NULL AND n.foo > 0 RETURN n"
    )
    assertRewritten(
      "MATCH (n) WHERE exists(n.prop) OR n.foo > 0 RETURN n",
      "MATCH (n) WHERE n.prop IS NOT NULL OR n.foo > 0 RETURN n"
    )
    assertRewritten(
      "MATCH (n) WHERE (exists(n.prop) OR n.foo > 0) AND n:N RETURN n",
      "MATCH (n) WHERE (n.prop IS NOT NULL OR n.foo > 0) AND n:N RETURN n"
    )
    assertRewritten(
      "MATCH (n WHERE exists(n.prop) AND n.foo > 0) RETURN n",
      "MATCH (n WHERE n.prop IS NOT NULL AND n.foo > 0) RETURN n"
    )
    assertRewritten(
      "MATCH (n WHERE exists(n.prop) OR n.foo > 0) RETURN n",
      "MATCH (n WHERE n.prop IS NOT NULL OR n.foo > 0) RETURN n"
    )
    assertRewritten(
      "MATCH (n WHERE (exists(n.prop) OR n.foo > 0) AND n:N) RETURN n",
      "MATCH (n WHERE (n.prop IS NOT NULL OR n.foo > 0) AND n:N) RETURN n"
    )
  }

  test("should not rewrite exists in with") {
    assertNotRewritten(
      "MATCH (n) WITH exists(n.prop) AS e RETURN e"
    )
  }

  test("should not rewrite exists in where if nested more complicated") {
    assertNotRewritten(
      "MATCH (n) WHERE exists(n.prop) = n.prop RETURN n"
    )
    assertNotRewritten(
      "MATCH (n WHERE exists(n.prop) = n.prop) RETURN n"
    )
  }

  test("should rewrite 01256 to 0o1256") {
    assertRewritten(
      "RETURN 01256 AS t",
      "RETURN 0o1256 AS t"
    )
  }

  test("should rewrite -01256 to -0o1256") {
    assertRewritten(
      "RETURN -01256 AS t",
      "RETURN -0o1256 AS t"
    )
  }
}
