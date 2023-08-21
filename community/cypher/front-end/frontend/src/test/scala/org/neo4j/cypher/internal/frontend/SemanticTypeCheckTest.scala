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

import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.ListCoercedToBooleanCheck
import org.neo4j.cypher.internal.frontend.phases.OpenCypherJavaCCParsing
import org.neo4j.cypher.internal.frontend.phases.PatternExpressionInNonExistenceCheck
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.SemanticTypeCheck
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SemanticTypeCheckTest extends CypherFunSuite {

  private val pipeline = OpenCypherJavaCCParsing andThen
    PreparatoryRewriting andThen
    SemanticAnalysis(warn = false) andThen
    SemanticTypeCheck

  // PatternExpressionInNonExistenceCheck
  test("should fail if pattern expression is used wherever we don't expect a boolean value") {
    val queries = Seq(
      "MATCH (a) RETURN (a)--()",
      "MATCH (a) WHERE ANY (x IN (a)--() WHERE 1=1) RETURN a"
    )

    queries.foreach { query =>
      withClue(s"Failing query: $query") {
        runPipeline(query).errors.map(_.msg) should contain(PatternExpressionInNonExistenceCheck.errorMessage)
      }
    }
  }

  test("should fail if pattern expression is used in size()") {
    val queries = Seq(
      "MATCH (a) RETURN size ( (a)-[]->() )",
      "MATCH (a) RETURN size ( (a)--() )"
    )

    queries.foreach { query =>
      withClue(s"Failing query: $query") {
        runPipeline(query).errors.map(_.msg) should contain(
          PatternExpressionInNonExistenceCheck.errorMessageForSizeFunction
        )
      }
    }
  }

  test("should not fail if pattern expression is used where we expect a boolean value") {
    val queries = Seq(
      "MATCH (a)--() RETURN a",
      "RETURN NOT ()--()",
      "MATCH (a) WHERE (a)--() RETURN a",
      "RETURN ()--() OR ()--()--()",
      "MATCH (a) RETURN [p=(a)--() | p]",
      "RETURN NOT exists(()--())",
      "MATCH (a) WHERE exists((a)--()) RETURN a",
      """
        |MATCH (actor:Actor)
        |RETURN actor,
        |  CASE
        |    WHEN (actor)-[:WON]->(:Oscar) THEN 'Oscar winner'
        |    WHEN (actor)-[:WON]->(:GoldenGlobe) THEN 'Golden Globe winner'
        |    ELSE 'None'
        |  END AS accolade
        |""".stripMargin,
      """
        |MATCH (movie:Movie)<-[:ACTED_IN]-(actor:Actor)
        |WITH movie, collect(actor) AS cast
        |WHERE ANY(actor IN cast WHERE (actor)-[:WON]->(:Award))
        |RETURN movie
        |""".stripMargin
    )

    queries.foreach { query =>
      withClue(s"Failing query: $query") {
        runPipeline(query).errors.size shouldBe 0
      }
    }
  }

  test("should fail if list is coerced to a boolean") {
    val queries = Seq(
      "RETURN NOT []",
      "RETURN NOT [1]",
      "RETURN NOT ['a']",
      "RETURN ['a'] OR []",
      "RETURN TRUE OR []",
      "RETURN NOT (TRUE OR [])",
      "RETURN ['a'] AND []",
      "RETURN TRUE AND []",
      "RETURN NOT (TRUE AND [])",
      "MATCH (n) WHERE [] RETURN TRUE",
      "MATCH (n) WHERE range(0, 10) RETURN TRUE",
      "MATCH (n) WHERE range(0, 10) RETURN range(0, 10)"
    )

    queries.foreach { query =>
      withClue(s"Failing query: $query") {
        runPipeline(query).errors.map(_.msg) should contain(ListCoercedToBooleanCheck.errorMessage)
      }
    }
  }

  test("should not fail to coerce pattern expressions to boolean") {
    val queries = Seq(
      "RETURN NOT FALSE",
      "RETURN NOT ()--()",
      "RETURN ()--() OR ()--()--()",
      "RETURN ()--() AND ()--()--()",
      "MATCH (n) WHERE (n)-[]->() RETURN n",
      "WITH 1 AS foo WHERE ()--() RETURN *",
      """
        |MATCH (a), (b)
        |WITH a, b
        |WHERE a.id = 0
        |  AND (a)-[:T]->(b:Label1)
        |  OR (a)-[:T*]->(b:Label2)
        |RETURN DISTINCT b
      """.stripMargin,
      """
        |MATCH (a), (b)
        |WITH a, b
        |WHERE a.id = 0
        |  AND exists((a)-[:T]->(b:Label1))
        |  OR exists((a)-[:T*]->(b:Label2))
        |RETURN DISTINCT b
      """.stripMargin,
      "MATCH (n) WHERE NOT (n)-[:REL2]-() RETURN n",
      "MATCH (n) WHERE (n)-[:REL1]-() AND (n)-[:REL3]-() RETURN n",
      "MATCH (n WHERE (n)--()) RETURN n",
      """
        |MATCH (actor:Actor)
        |RETURN actor,
        |  CASE
        |    WHEN (actor)-[:WON]->(:Oscar) THEN 'Oscar winner'
        |    WHEN (actor)-[:WON]->(:GoldenGlobe) THEN 'Golden Globe winner'
        |    ELSE 'None'
        |  END AS accolade
        |""".stripMargin,
      """
        |MATCH (movie:Movie)<-[:ACTED_IN]-(actor:Actor)
        |WITH movie, collect(actor) AS cast
        |WHERE ANY(actor IN cast WHERE (actor)-[:WON]->(:Award))
        |RETURN movie
        |""".stripMargin
    )

    queries.foreach { query =>
      withClue(s"Failing query: size($query)") {
        runPipeline(query).errors.size shouldBe 0
      }
    }
  }

  private def runPipeline(query: String): ErrorCollectingContext = {
    val startState = InitialState(query, None, NoPlannerName, new AnonymousVariableNameGenerator)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context
  }
}
