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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class ExistsExpressionSemanticAnalysisTest
    extends CypherFunSuite
    with SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"$testName"

  test("""MATCH (a)
         |RETURN EXISTS { CREATE (b) }
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "An Exists Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (m)
         |WHERE EXISTS { OPTIONAL MATCH (a)-[r]->(b) }
         |RETURN m
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "Query cannot conclude with MATCH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
        InputPosition(25, 2, 16)
      )
    )
  }

  test("""MATCH (m)
         |WHERE EXISTS { MATCH (a:A)-[r]->(b) USING SCAN a:A }
         |RETURN m
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "Query cannot conclude with MATCH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
        InputPosition(25, 2, 16)
      )
    )
  }

  test("""MATCH (a)
         |RETURN EXISTS { SET a.name = 1 }
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "An Exists Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (a)
         |RETURN EXISTS { MATCH (b) WHERE b.a = a.a DETACH DELETE b }
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "An Exists Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (a)
         |RETURN EXISTS { MATCH (b) MERGE (b)-[:FOLLOWS]->(:Person) }
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "An Exists Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (a)
         |RETURN EXISTS { CALL db.createLabel("CAT") }
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "An Exists Expression cannot contain any updates",
        InputPosition(17, 2, 8)
      )
    )
  }

  test("""MATCH (a)
         |RETURN EXISTS {
         |   MATCH (a)-[:KNOWS]->(b)
         |   RETURN b.name as name
         |   UNION ALL
         |   MATCH (a)-[:LOVES]->(b)
         |   RETURN b.name as name
         |}""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldBe empty
  }

  test("""MATCH (a)
         |RETURN EXISTS { MATCH (m)-[r]->(p), (a)-[r2]-(c) }
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("""MATCH (a)
         |RETURN EXISTS { (a)-->(b) WHERE b.prop = 5  }
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("""WITH 5 as aNum
         |MATCH (a)
         |RETURN EXISTS {
         |  WITH 6 as aNum
         |  MATCH (a)-->(b) WHERE b.prop = aNum
         |  RETURN a
         |}
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "The variable `aNum` is shadowing a variable with the same name from the outer scope and needs to be renamed",
        InputPosition(53, 4, 13)
      )
    )
  }

  test("""WITH 5 as aNum
         |MATCH (a)
         |RETURN EXISTS {
         |  MATCH (a)-->(b) WHERE b.prop = aNum
         |  WITH 6 as aNum
         |  MATCH (b)-->(c) WHERE c.prop = aNum
         |  RETURN a
         |}
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "The variable `aNum` is shadowing a variable with the same name from the outer scope and needs to be renamed",
        InputPosition(91, 5, 13)
      )
    )
  }

  test("""MATCH (a)
         |RETURN EXISTS {
         |  MATCH (a)-->(b)
         |  WITH b as a
         |  MATCH (b)-->(c)
         |  RETURN a
         |}
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldEqual Set(
      SemanticError(
        "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
        InputPosition(56, 4, 13)
      )
    )
  }

  test("""MATCH (a)
         |RETURN EXISTS {
         |  MATCH (a)-->(b)
         |  WITH b as c
         |  MATCH (c)-->(d)
         |  RETURN a
         |}
         |""".stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldBe Set.empty
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    RETURN CASE
         |       WHEN true THEN 1
         |       ELSE 2
         |    END
         |}
         |RETURN person.name
     """.stripMargin) {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.FullExistsSupport).errors.toSet shouldBe Set.empty
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         |    MATCH (person)-[:HAS_FRIEND]-(friend:Person)
         |    RETURN person
         |}
         |RETURN person.name
     """.stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        "Exists Expressions containing a regular query are not yet supported",
        InputPosition(28, 2, 7)
      )
    )
  }
}
