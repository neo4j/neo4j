/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodePatternPredicateSemanticAnalysisTest extends CypherFunSuite {

  private val pipeline = JavaccParsing andThen SemanticAnalysis(warn = true) andThen SemanticAnalysis(warn = false)

  test("should allow node pattern predicates in MATCH") {
    val query = "WITH 123 AS minValue MATCH (n {prop: 42} WHERE n.otherProp > minValue)-->(m:Label WHERE m.prop = 42) RETURN n AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("should not allow node pattern predicates in MATCH to refer to other nodes") {
    val query = "MATCH (start)-->(end:Label WHERE start.prop = 42) RETURN start AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Variable `start` not defined"
    )
  }

  test("should not allow node pattern predicates in CREATE") {
    val query = "CREATE (n WHERE n.prop = 123)"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Node pattern predicates are not allowed in CREATE"
    )
  }

  test("should not allow node pattern predicates in MERGE") {
    val query = "MERGE (n WHERE n.prop = 123)"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Node pattern predicates are not allowed in MERGE"
    )
  }

  test("should allow node pattern predicates in pattern comprehension") {
    val query = "WITH 123 AS minValue RETURN [(n {prop: 42} WHERE n.otherProp > minValue)-->(m:Label WHERE m.prop = 42) | n] AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("should not allow node pattern predicates in pattern comprehension to refer to other nodes") {
    val query = "RETURN [(start)-->(end:Label WHERE start.prop = 42) | start] AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Variable `start` not defined"
    )
  }

  test("should not allow node pattern predicates in pattern expression") {
    val query =
      """MATCH (a), (b)
        |RETURN exists((a WHERE a.prop > 123)-->(b)) AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Node pattern predicates are not allowed in expression"
    )
  }

  test("should allow node pattern predicates in MATCH with shortestPath") {
    val query =
      """
        |WITH 123 AS minValue
        |MATCH p = shortestPath((n {prop: 42} WHERE n.otherProp > minValue)-[:REL*]->(m:Label WHERE m.prop = 42))
        |RETURN n AS result
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("should not allow node pattern predicates in MATCH with shortestPath to refer to other nodes") {
    val query =
      """
        |MATCH p = shortestPath((start)-[:REL*]->(end:Label WHERE start.prop = 42))
        |RETURN start AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Variable `start` not defined"
    )
  }

  test("should not allow node pattern predicates in shortestPath expression") {
    val query =
      """
        |MATCH (a), (b)
        |WITH shortestPath((a WHERE a.prop > 123)-[:REL*]->(b)) AS p
        |RETURN length(p) AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Node pattern predicates are not allowed in expression"
    )
  }

  private def initStartState(query: String) =
    InitialState(query, None, NoPlannerName, new AnonymousVariableNameGenerator)
}
