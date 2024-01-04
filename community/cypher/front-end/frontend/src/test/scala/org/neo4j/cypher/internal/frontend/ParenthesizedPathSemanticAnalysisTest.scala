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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class ParenthesizedPathSemanticAnalysisTest extends SemanticAnalysisTestSuite {

  implicit private val windowsStringSafe: WindowsStringSafe.type = WindowsStringSafe

  private val gpmShortestPath: Seq[SemanticFeature] = Seq(SemanticFeature.GpmShortestPath)

  test("can use sub-path variable in WHERE") {
    val q =
      """
        |MATCH SHORTEST 1 (p = (a)-[r]->+(b) WHERE length(p) % 2 = 0)
        |RETURN b
        |""".stripMargin

    runSemanticAnalysisWithSemanticFeatures(gpmShortestPath, q).errorMessages shouldBe empty
  }

  test("can not use path variable from the same MATCH clause in WHERE") {
    val q =
      """
        |MATCH p = SHORTEST 1 ((a)-[r]->+(b) WHERE length(p) % 2 = 0)
        |RETURN b
        |""".stripMargin

    runSemanticAnalysisWithSemanticFeatures(gpmShortestPath, q).errorMessages.single shouldEqual
      """From within a parenthesized path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, p is defined in the same `MATCH` clause as ((a) (()-[r]->())+ (b) WHERE length(p) % 2 = 0).""".stripMargin
  }

  test("can use path variable from a previous MATCH clause in WHERE") {
    val q =
      """
        |MATCH p = (x)-->(y)
        |MATCH SHORTEST 1 ((a)-[r]->+(b) WHERE length(p) % 2 = 0)
        |RETURN b
        |""".stripMargin

    runSemanticAnalysisWithSemanticFeatures(gpmShortestPath, q).errorMessages shouldBe empty
  }

  test("can not use a variable from the same MATCH clause in a subquery expression") {
    val q =
      """
        |MATCH p = SHORTEST 1 ((a)-[r]->+(b) WHERE 0 = COUNT { (x)-->(y) WHERE length(p) % 2 = 0} )
        |RETURN b
        |""".stripMargin

    runSemanticAnalysisWithSemanticFeatures(gpmShortestPath, q).errorMessages.single shouldEqual
      """From within a parenthesized path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, p is defined in the same `MATCH` clause as ((a) (()-[r]->())+ (b) WHERE 0 = COUNT { MATCH (x)-->(y)
        |  WHERE length(p) % 2 = 0 }).""".stripMargin
  }

  implicit private class SingleSyntax(errorMessages: Seq[String]) {

    def single: String = {
      errorMessages.size shouldBe 1
      errorMessages.head
    }
  }
}
