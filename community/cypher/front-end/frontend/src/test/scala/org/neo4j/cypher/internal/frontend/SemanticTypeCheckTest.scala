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
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.ListCoercedToBooleanCheck
import org.neo4j.cypher.internal.frontend.phases.Parse
import org.neo4j.cypher.internal.frontend.phases.PatternExpressionInNonExistenceCheck
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.SemanticTypeCheck
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.messages.MessageUtilProvider
import org.scalatest.LoneElement

class SemanticTypeCheckTest extends CypherFunSuite with LoneElement {

  private def pipeline(cypherVersion: CypherVersion) = Parse(useAntlr = true, cypherVersion) andThen
    PreparatoryRewriting andThen
    SemanticAnalysis(warn = false) andThen
    SemanticTypeCheck(cypherVersion)

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

  private case class SelfReferenceAcrossPatternsTestQueries(disallowed: Seq[String], allowed: Seq[String])

  private object SelfReferenceAcrossPatternsTestQueries {

    def forClause(clause: String): SelfReferenceAcrossPatternsTestQueries = {
      val disallowed = Seq(
        s"$clause (a {foo:1}), (b {foo:a.foo})",
        s"$clause (b {prop: a.prop}), (a)",
        s"$clause (a), (b)-[r: REL {prop: a.prop}]->(c)",
        s"$clause (b)-[r: REL {prop: a.prop}]->(c), (a)",
        s"$clause (b)-[a: REL]->(c), (d {prop:a.prop})",
        s"$clause (a), (b {prop: EXISTS {(a)-->()}})",
        s"$clause (b {prop: EXISTS {(a)-->()}}), (a)",
        s"$clause (a), (a)-[:REL]->({prop:a.prop})",
        s"$clause (a), (b {prop: labels(a)})",
        s"$clause (a), (b {prop: true IN [x IN labels(a) | true]})"
      )

      val allowed = Seq(
        s"MATCH (n) $clause (a {prop: n.prop})",
        s"MATCH (a) $clause (a)-[:REL]->({prop:a.prop})",
        s"$clause (a)-[:REL]->(a)",
        s"$clause (a), (a)-[:REL]->(b)",

        // These cases are shadowing and not references so should not be disallowed
        s"$clause (n {prop: true IN [n IN [false] | true]})",
        s"$clause (n {prop: true IN [n IN [false] | n]})",
        s"$clause (a)-[r:R {prop: true IN [r IN [false] | true]}]->(b)",
        s"$clause (a)-[r:R {prop: true IN [r IN [false] | r]}]->(b)",
        s"$clause (a)-[r:R {prop: true IN [a IN [false] | a]}]->(b)",
        s"$clause (a)-[r:R]->(b {prop: true IN [r IN [false] | r]})",
        s"$clause (a)-[r:R]->(b {prop: true IN [a IN [false] | a]})",
        s"MATCH p=()-[]->() $clause (a)-[r:R {prop: true IN [a in nodes(p) | a.prop = 1]}]->(b)"
      )

      SelfReferenceAcrossPatternsTestQueries(disallowed, allowed)
    }
  }

  private def runReferenceAcrossPatternsTests(clause: String, cypherVersion: CypherVersion): Unit = {
    val queries = SelfReferenceAcrossPatternsTestQueries.forClause(clause)

    val errorMessage =
      s"Creating an entity (a) and referencing that entity in a property definition in the same $clause is not allowed. Only reference variables created in earlier clauses."

    for (query <- queries.disallowed) {
      withClue(s"Failing query: $query") {
        runPipeline(query, cypherVersion).errors.loneElement.msg shouldEqual errorMessage
      }
    }

    for (query <- queries.allowed) {
      withClue(s"Failing query: $query") {
        runPipeline(query, cypherVersion).errors should be(empty)
      }
    }
  }

  test("property references across patterns should not be allowed in INSERT ") {
    runReferenceAcrossPatternsTests("INSERT", CypherVersion.Cypher5)
    runReferenceAcrossPatternsTests("INSERT", CypherVersion.Cypher6)
  }

  test("property references across patterns should be allowed in CREATE, CYPHER 5") {
    val queries = SelfReferenceAcrossPatternsTestQueries.forClause("CREATE")
    for (query <- queries.disallowed ++ queries.allowed) {
      withClue(s"Failing query: $query") {
        runPipeline(query, CypherVersion.Cypher5).errors should be(empty)
      }
    }
  }

  test("property references across patterns should not be allowed in CREATE, CYPHER 6") {
    runReferenceAcrossPatternsTests("CREATE", CypherVersion.Cypher6)
  }

  private case class SelfReferenceWithinPatternTestQueries(
    disallowedNode: Seq[String],
    disallowedRel: Seq[String],
    allowed: Seq[String]
  )

  private object SelfReferenceWithinPatternTestQueries {

    def forClause(clause: String): SelfReferenceWithinPatternTestQueries = {
      val disallowedNode = Seq(
        s"$clause (a {prop:'p'})-[:T]->(b {prop:a.prop})",
        s"$clause (a {prop:'p'})<-[:T]-(b {prop:a.prop})",
        s"$clause (a {prop:'p'})-[:T]-(b {prop:a.prop})",
        s"CREATE ({prop:'p'})-[:T]->({prop:'p'}) $clause (b {prop:a.prop})-[:T]->(a {prop:'p'})",
        s"$clause (a {prop:'p'})-[b:T {prop:a.prop}]->()",
        s"FOREACH (x in [1,2,3] | $clause (a {prop:'p'})-[:R]-(b {prop:a.prop}))"
      )

      val disallowedRel = Seq(
        s"$clause ()-[a:T {prop:'p'}]->()<-[b :S {prop:a.prop}]-()"
      )

      val allowed = Seq(
        s"MATCH (a {prop:'p'}) $clause (b {prop:a.prop})",
        s"$clause (a {prop:'p'}) $clause (a)-[:T]->(b {prop:a.prop})"
      )
      SelfReferenceWithinPatternTestQueries(disallowedNode, disallowedRel, allowed)
    }
  }

  private def runReferenceWithinPatternTests(clause: String, cypherVersion: CypherVersion): Unit = {
    val queries = SelfReferenceWithinPatternTestQueries.forClause(clause)

    for (query <- queries.disallowedNode) {
      withClue(s"Failing query: $query") {
        runPipeline(query, cypherVersion).errors.map(e => e.msg) should
          contain(MessageUtilProvider.createSelfReferenceError("a", "Node", clause))
      }
    }
    for (query <- queries.disallowedRel) {
      withClue(s"Failing query: $query") {
        runPipeline(query, cypherVersion).errors.map(e => e.msg) should
          contain(MessageUtilProvider.createSelfReferenceError("a", "Relationship", clause))
      }
    }

    for (query <- queries.allowed) {
      withClue(s"Failing query: $query") {
        runPipeline(query, cypherVersion).errors should be(empty)
      }
    }
  }

  test("property references within patterns should not be allowed in INSERT") {
    runReferenceWithinPatternTests("INSERT", CypherVersion.Cypher5)
    runReferenceWithinPatternTests("INSERT", CypherVersion.Cypher6)
  }

  test("property references within patterns should not be allowed in CREATE") {
    runReferenceWithinPatternTests("CREATE", CypherVersion.Cypher5)
    runReferenceWithinPatternTests("CREATE", CypherVersion.Cypher6)
  }

  test("property references within patterns should be allowed in MERGE, CYPHER 5") {
    val queries = SelfReferenceWithinPatternTestQueries.forClause("MERGE")
    for (query <- queries.allowed ++ queries.disallowedNode ++ queries.disallowedRel) {
      withClue(s"Failing query: $query") {
        runPipeline(query, CypherVersion.Cypher5).errors should be(empty)
      }
    }
  }

  test("property references within patterns should not be allowed in MERGE, CYPHER 6") {
    runReferenceWithinPatternTests("MERGE", CypherVersion.Cypher6)
  }

  private def runPipeline(
    query: String,
    cypherVersion: CypherVersion = CypherVersion.Default
  ): ErrorCollectingContext = {
    val startState = InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)
    val context = new ErrorCollectingContext() {
      override def errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
    }
    pipeline(cypherVersion).transform(startState, context)

    context
  }
}
