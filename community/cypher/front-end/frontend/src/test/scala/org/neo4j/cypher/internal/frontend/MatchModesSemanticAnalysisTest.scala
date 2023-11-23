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
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class MatchModesSemanticAnalysisTest extends CypherFunSuite with SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"MATCH $testName RETURN *"

  def errorsFromSemanticAnalysis: Seq[SemanticErrorDef] = {
    runSemanticAnalysisWithSemanticFeatures(
      SemanticFeature.GpmShortestPath,
      SemanticFeature.MatchModes
    ).errors
  }

  def unboundRepeatableElementsSemanticError(pos: InputPosition): SemanticError = SemanticError(
    "The pattern may yield an infinite number of rows under match mode REPEATABLE ELEMENTS, " +
      "perhaps use a path selector or add an upper bound to your quantified path patterns.",
    pos
  )

  def differentRelationshipsSelectivePathPatternSemanticError(
    pos: InputPosition,
    semanticFeatureEnabled: Boolean = true
  ): SemanticError = {
    val matchModeTip = if (semanticFeatureEnabled) {
      " You may want to use multiple MATCH clauses, or you might want to consider using the REPEATABLE ELEMENTS match mode."
    } else {
      ""
    }

    SemanticError(
      "Multiple path patterns cannot be used in the same clause in combination with a selective path selector." + matchModeTip,
      pos
    )
  }

  test("DIFFERENT RELATIONSHIPS (a)") {
    // running without semantic feature should fail
    runSemanticAnalysis(defaultQuery).errors.map(_.msg) shouldEqual Seq(
      "Match modes such as `DIFFERENT RELATIONSHIPS` are not supported yet."
    )
  }

  test("(a)") {
    // running with implicit "DIFFERENT RELATIONSHIPS" match mode should not fail
    runSemanticAnalysis(defaultQuery).errors.map(_.msg) shouldBe empty
  }

  test("REPEATABLE ELEMENTS (a)") {
    // running without semantic feature should fail
    runSemanticAnalysis(defaultQuery).errors.map(_.msg) shouldEqual Seq(
      "Match modes such as `REPEATABLE ELEMENTS` are not supported yet."
    )
  }

  test("DIFFERENT RELATIONSHIPS ((a)-[:REL]->(b)){2}") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("((a)-[:REL]->(b)){2}, ((c)-[:REL]->(d))+") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b)){2}") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b)){1,}") {
    errorsFromSemanticAnalysis shouldEqual Seq(
      unboundRepeatableElementsSemanticError(InputPosition(26, 1, 27))
    )
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b))+") {
    errorsFromSemanticAnalysis shouldEqual Seq(
      unboundRepeatableElementsSemanticError(InputPosition(26, 1, 27))
    )
  }

  test("REPEATABLE ELEMENTS (a)-[:REL*]->(b)") {
    errorsFromSemanticAnalysis shouldEqual Seq(
      unboundRepeatableElementsSemanticError(InputPosition(26, 1, 27))
    )
  }

  test("REPEATABLE ELEMENTS SHORTEST 2 PATH ((a)-[:REL]->(b))+") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("REPEATABLE ELEMENTS ANY ((a)-[:REL]->(b))+") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("REPEATABLE ELEMENTS SHORTEST 1 PATH GROUPS ((a)-[:REL]->(b))+") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("shortestPath((a)-[:REL*]->(b)), shortestPath((c)-[:REL*]->(d))") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b)){2}, ((c)-[:REL]->(d))+") {
    errorsFromSemanticAnalysis shouldEqual Seq(
      unboundRepeatableElementsSemanticError(InputPosition(48, 1, 49))
    )
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b))+, ((c)-[:REL]->(d))+") {
    errorsFromSemanticAnalysis shouldEqual Seq(
      unboundRepeatableElementsSemanticError(InputPosition(26, 1, 27)),
      unboundRepeatableElementsSemanticError(InputPosition(46, 1, 47))
    )
  }

  test("REPEATABLE ELEMENTS SHORTEST 1 PATH (a)-[:REL*]->(b), SHORTEST 1 PATH (c)-[:REL*]->(d)") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("DIFFERENT RELATIONSHIPS SHORTEST 1 PATH (a)-[:REL*]->(b), SHORTEST 1 PATH (c)-[:REL*]->(d)") {
    errorsFromSemanticAnalysis shouldEqual Seq(
      differentRelationshipsSelectivePathPatternSemanticError(InputPosition(46, 1, 47))
    )
  }

  test("SHORTEST 1 PATH (a)-[:REL*]->(b), (c)-[:REL]->(d)") {
    errorsFromSemanticAnalysis shouldEqual Seq(
      differentRelationshipsSelectivePathPatternSemanticError(InputPosition(22, 1, 23))
    )
  }

  test("SHORTEST 1 PATH (a)-[:REL*]->(b), (c)-[:REL]->(e)") {
    // running without SemanticFeature.MatchModes
    runSemanticAnalysisWithSemanticFeatures(
      SemanticFeature.GpmShortestPath
    ).errors shouldEqual Seq(
      differentRelationshipsSelectivePathPatternSemanticError(InputPosition(22, 1, 23), semanticFeatureEnabled = false)
    )
  }

  test("(a)-[:REL]->(b), ALL PATHS (c)-[:REL]->(d)") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test("REPEATABLE ELEMENTS (a)-[:REL]->(b), ALL PATHS (c)-[:REL]->(d)") {
    errorsFromSemanticAnalysis shouldBe empty
  }

  test(s"MATCH REPEATABLE ELEMENTS shortestPath((a)-->(b)) RETURN *") {
    val result = runSemanticAnalysisWithSemanticFeatures(Seq(SemanticFeature.MatchModes), testName)
    result.errorMessages shouldBe Seq(
      "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
    )
  }

  test(s"MATCH DIFFERENT RELATIONSHIPS shortestPath((a)-->(b)) RETURN *") {
    val result = runSemanticAnalysisWithSemanticFeatures(Seq(SemanticFeature.MatchModes), testName)
    result.errorMessages shouldBe Seq(
      "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
    )
  }

  test(s"MATCH REPEATABLE ELEMENTS allShortestPaths((a)-->(b)) RETURN *") {
    val result = runSemanticAnalysisWithSemanticFeatures(Seq(SemanticFeature.MatchModes), testName)
    result.errorMessages shouldBe Seq(
      "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
    )
  }

  test(s"MATCH REPEATABLE ELEMENTS (a)-->(b) WHERE shortestPath((a)-->(b)) IS NOT NULL RETURN *") {
    val result = runSemanticAnalysisWithSemanticFeatures(Seq(SemanticFeature.MatchModes), testName)
    result.errorMessages shouldBe Seq(
      "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
    )
  }

  test(s"MATCH REPEATABLE ELEMENTS (a)-->(b) WHERE EXISTS { MATCH shortestPath((a)-->(b)) } RETURN *") {
    val result = runSemanticAnalysisWithSemanticFeatures(Seq(SemanticFeature.MatchModes), testName)
    result.errorMessages shouldBe Seq(
      "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
    )
  }

  test(s"CALL { MATCH REPEATABLE ELEMENTS (a)-->(b) MATCH shortestPath((c)-->(d)) RETURN * } RETURN *") {
    val result = runSemanticAnalysisWithSemanticFeatures(Seq(SemanticFeature.MatchModes), testName)
    result.errorMessages shouldBe Seq(
      "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
    )
  }

  test(s"MATCH REPEATABLE ELEMENTS (a)-->(b) MATCH shortestPath((c)-->(d)) RETURN *") {
    val result = runSemanticAnalysisWithSemanticFeatures(Seq(SemanticFeature.MatchModes), testName)
    result.errorMessages shouldBe empty
  }

  test(s"MATCH DIFFERENT RELATIONSHIPS (a)-->(b) MATCH shortestPath((c)-->(d)) RETURN *") {
    val result = runSemanticAnalysisWithSemanticFeatures(Seq(SemanticFeature.MatchModes), testName)
    result.errorMessages shouldBe empty
  }
}
