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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SetClauseSemanticAnalysisTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  test("MATCH (n) SET n[\"prop\"] = 3") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "Setting properties dynamically is not supported.",
        InputPosition(16, 1, 17).withInputLength(6)
      )
    )
  }

  test("MATCH (n)-[r]->(m) SET (CASE WHEN n.prop = 5 THEN n ELSE r END)[\"prop\"] = 3") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "Setting properties dynamically is not supported.",
        InputPosition(64, 1, 65).withInputLength(6)
      )
    )
  }

  test("MATCH (n), (m) SET n[1] = 3") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldEqual Set(
      SemanticError(
        "Type mismatch: node or relationship property key must be given as String, but was Integer",
        InputPosition(21, 1, 22)
      )
    )
  }

  test("MATCH (n)-[r]->(m) SET r[5.0] = 3") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldEqual Set(
      SemanticError(
        "Type mismatch: node or relationship property key must be given as String, but was Float",
        InputPosition(25, 1, 26)
      )
    )
  }

  test("WITH 5 AS var MATCH (n) SET n[var] = 3") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldEqual Set(
      SemanticError(
        "Type mismatch: node or relationship property key must be given as String, but was Integer",
        InputPosition(30, 1, 31)
      )
    )
  }

  test("WITH {key: 1} AS var SET var['key'] = 3") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldEqual Set(
      SemanticError(
        "Type mismatch: expected Node or Relationship but was Map",
        InputPosition(25, 1, 26)
      )
    )
  }

  test("WITH \"prop\" as prop MERGE (n) ON MATCH SET n[prop] = 3 ON CREATE SET n[prop] = 4") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "Setting properties dynamically is not supported.",
        InputPosition(45, 1, 46)
      ),
      SemanticError(
        "Setting properties dynamically is not supported.",
        InputPosition(71, 1, 72)
      )
    )
  }

  test("MATCH (n) WITH {key: n} AS var SET (var.key)['prop'] = 3") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }

  test("MATCH (n) SET n[\"prop\"] = 4") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }

  test("MATCH (n) SET n[$param] = 'hi'") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }

  test("MATCH ()-[r]->() SET r[\"prop\"] = 4") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }

  test("MATCH (n) SET n.prop = 4") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }

  test("MATCH (n)-[r]->() SET n = r") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }

  test("MATCH (n)-[r]->() SET n = properties(r)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }

  test("MATCH (n)-[r]->() SET n = {prop: 1}") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }

  test("MATCH (n)-[r]->() SET n += {prop: 1}") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors.toSet shouldBe empty
  }
}
