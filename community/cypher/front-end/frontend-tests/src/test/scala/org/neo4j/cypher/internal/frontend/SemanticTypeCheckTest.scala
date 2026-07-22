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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionHelpers.equalInAllVersions
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticError.errorMessageForSizeFunction
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.RewritePhaseTest.reanalyze
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ListCoercedToBooleanCheck
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticTypeCheck
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class SemanticTypeCheckTest extends CypherFunSuite with CypherVersionTestSupport {

  private def pipeline = Parse andThen
    PreparatoryRewriting andThen
    reanalyze andThen
    SemanticTypeCheck

  // PatternExpressionInNonExistenceCheck
  test("should fail if pattern expression is used wherever we don't expect a boolean value") {
    val queries5 = Seq(
      "MATCH (a) RETURN (a)--()",
      "MATCH (a) WITH a, (a)--() AS hasNeighbor RETURN a, hasNeighbor",
      "MATCH (a) WHERE ANY (x IN (a)--() WHERE 1=1) RETURN a"
    )
    val queries25plus = queries5 ++ Seq(
      "MATCH (a) LET hasNeighbor = (a)--() RETURN a, hasNeighbor"
    )

    val expectedStatusDescription =
      "error: syntax error or access rule violation - invalid use of pattern expression. " +
        "A pattern expression can only be used to test the existence of a pattern. Use a pattern comprehension instead."

    def run(query: String, version: CypherVersion) =
      withClue(s"Failing query: $query") {
        val errors: Seq[SemanticErrorDef] = runPipeline(version, query)
        errors.map(_.msg) should contain(SemanticError.invalidUseOfPatternExpressionMessage)
        errors.map(_.gqlStatusObject).exists(e =>
          e.gqlStatus() == "42001" &&
            e.statusDescription() == "error: syntax error or access rule violation - invalid syntax" &&
            e.cause().isPresent &&
            e.cause().get().gqlStatus() == "42I34" &&
            e.cause().get().statusDescription() == expectedStatusDescription &&
            e.cause().get().cause().isEmpty
        ) shouldBe true
      }

    queries5.foreach { query => run(query, CypherVersion.Cypher5) }
    queries25plus.foreach { query => run(query, CypherVersion.Cypher25) }
  }

  test("should fail if pattern expression is used in size()") {
    val queries = Seq(
      "MATCH (a) RETURN size ( (a)-[]->() )",
      "MATCH (a) RETURN size ( (a)--() )"
    )

    queries.foreach { query =>
      withClue(s"Failing query: $query") {
        val errors = runPipeline(query).map(e => e.asInstanceOf[SemanticError])
        errors should have length 1
        val error = errors.head

        error.msg should be(errorMessageForSizeFunction)

        error.gqlStatusObject should be(
          gqlStatus(
            GqlStatusInfoCodes.STATUS_42001,
            "error: syntax error or access rule violation - invalid syntax"
          ).withCause(
            GqlStatusInfoCodes.STATUS_42I52,
            s"error: syntax error or access rule violation - no longer valid syntax. $errorMessageForSizeFunction"
          )
        )
      }
    }
  }

  test("should not fail if pattern expression is used where we expect a boolean value") {
    val queries5 = Seq(
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

    val queries25plus = queries5 ++ Seq(
      "MATCH (a) FILTER (a)--() RETURN a",
      "MATCH (a) FILTER WHERE (a)--() RETURN a",
      "MATCH (a) LET hasNeighbor = EXISTS { (a)--() } RETURN a, hasNeighbor"
    )

    def run(query: String, version: CypherVersion) =
      withClue(s"Failing query: $query") {
        runPipeline(version, query).size shouldBe 0
      }

    queries5.foreach { query => run(query, CypherVersion.Cypher5) }
    queries25plus.foreach { query => run(query, CypherVersion.Cypher25) }
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
        runPipeline(query).map(_.msg) should contain(ListCoercedToBooleanCheck.errorMessage)
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
        runPipeline(query).size shouldBe 0
      }
    }
  }

  private def runPipeline(query: String): Seq[SemanticErrorDef] = equalInAllVersions(runPipeline(_, query))

  private def runPipeline(
    version: CypherVersion,
    query: String
  ): Seq[SemanticErrorDef] = {
    val startState = InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)
    val context = new ErrorCollectingContext(version, query = query) {
      override def errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
    }
    pipeline.transform(startState, context)

    context.errors
  }

}
