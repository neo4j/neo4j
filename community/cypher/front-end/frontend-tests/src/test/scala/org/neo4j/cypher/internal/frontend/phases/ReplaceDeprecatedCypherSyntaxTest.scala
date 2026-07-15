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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SyntaxDeprecationWarningsAndReplacements
import org.neo4j.cypher.internal.rewriting.Deprecations.SemanticallyDeprecatedFeatures
import org.neo4j.cypher.internal.rewriting.Deprecations.SyntacticallyDeprecatedFeatures
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ReplaceDeprecatedCypherSyntaxTest extends CypherFunSuite with AstConstructionTestSupport with RewritePhaseTest {

  override val rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    SyntaxDeprecationWarningsAndReplacements(SyntacticallyDeprecatedFeatures) andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = Some(true)) andThen
      SyntaxDeprecationWarningsAndReplacements(SemanticallyDeprecatedFeatures)

  test("should rewrite legacy relationship type disjunction") {
    assertRewritten(
      "MATCH (a)-[:A|:B|:C]-() RETURN a",
      "MATCH (a)-[:A|B|C]-() RETURN a"
    )
  }

  test("should rewrite setting of relationship properties to use properties function in a Set Clause") {
    assertRewritten(
      "MATCH (g)-[r:KNOWS]->(k) SET g = r",
      "MATCH (g)-[r:KNOWS]->(k) SET g = properties(r)",
      excludedVersions = Set(CypherVersion.Cypher25)
    )
  }

  test("should rewrite setting of node properties to use properties function in a Set Clause") {
    assertRewritten(
      "MATCH (g)-[r:KNOWS]->(k) SET g = k",
      "MATCH (g)-[r:KNOWS]->(k) SET g = properties(k)",
      excludedVersions = Set(CypherVersion.Cypher25)
    )
  }

  test("should rewrite setting of relationship properties to use properties function in a Mutate Set Clause") {
    assertRewritten(
      "MATCH (g)-[r:KNOWS]->(k) SET g += r",
      "MATCH (g)-[r:KNOWS]->(k) SET g += properties(r)",
      excludedVersions = Set(CypherVersion.Cypher25)
    )
  }

  test("should rewrite setting of node properties to use properties function in a Mutate Set Clause") {
    assertRewritten(
      "MATCH (g)-[r:KNOWS]->(k) SET g += k",
      "MATCH (g)-[r:KNOWS]->(k) SET g += properties(k)",
      excludedVersions = Set(CypherVersion.Cypher25)
    )
  }
}
