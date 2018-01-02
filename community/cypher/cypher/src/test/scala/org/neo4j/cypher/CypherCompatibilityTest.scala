/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher

import org.neo4j.kernel.api.exceptions.Status

class CypherCompatibilityTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport {

  val QUERY_2_X_ONLY = "MATCH (n:Label) RETURN n"
  val QUERY_1_9_ONLY = "START n=node(*) RETURN n.prop?"
  val QUERY_FOR_BOTH = "START n=node(*) RETURN n"

  test("should_accept_1_9_queries_with_db_config") {
    runWithConfig("cypher_parser_version" -> "1.9") {
      engine =>
        val result = engine.execute(QUERY_1_9_ONLY)
        result.toList // Does not throw
    }
  }

  test("should_accept_1_9_queries_using_query_prologue") {
    runWithConfig() {
      engine =>
        val result = engine.execute(s"CYPHER 1.9 $QUERY_1_9_ONLY")
        result.toList // Does not throw
    }
  }

  test("should_be_able_to_switch_between_versions") {
    runWithConfig() {
      engine =>
        engine.execute(s"CYPHER 1.9 $QUERY_FOR_BOTH").toList shouldBe empty
        engine.execute(s"CYPHER 2.2 $QUERY_FOR_BOTH").toList shouldBe empty
        engine.execute(s"CYPHER 2.3 $QUERY_FOR_BOTH").toList shouldBe empty
    }
  }

  test("should_be_able_to_switch_between_versions2") {
    runWithConfig() {
      engine =>
        engine.execute(s"CYPHER 2.3 $QUERY_FOR_BOTH").toList shouldBe empty
        engine.execute(s"CYPHER 2.2 $QUERY_FOR_BOTH").toList shouldBe empty
        engine.execute(s"CYPHER 1.9 $QUERY_FOR_BOTH") shouldBe empty
    }
  }

  test("should_be_able_to_override_config") {
    runWithConfig("cypher_parser_version" -> "2.2") {
      engine =>
        engine.execute(s"CYPHER 1.9 $QUERY_1_9_ONLY").toList shouldBe empty
    }
  }

  test("should_be_able_to_override_config2") {
    runWithConfig("cypher_parser_version" -> "1.9") {
      engine =>
        engine.execute(s"CYPHER 2.2 $QUERY_2_X_ONLY").toList shouldBe empty
    }
  }

  test("should_use_default_version_by_default") {
    runWithConfig() {
      engine =>
        engine.execute(QUERY_2_X_ONLY).toList shouldBe empty
    }
  }

  test("should handle profile") {
    runWithConfig() {
      (engine: ExecutionEngine) =>
        assertProfiled(engine, "CYPHER 1.9 PROFILE START n=node(*) RETURN n")
        assertProfiled(engine, "CYPHER 2.2 PROFILE MATCH n RETURN n")
        assertProfiled(engine, "CYPHER 2.3 runtime=interpreted PROFILE MATCH n RETURN n")
    }
  }

  test("should allow the use of explain only in the supported compilers") {
    runWithConfig() {
      engine =>
        intercept[InvalidArgumentException](engine.execute("CYPHER 1.9 EXPLAIN MATCH n RETURN n"))
        assertExplained(engine, "CYPHER 2.2 EXPLAIN MATCH n RETURN n")
        assertExplained(engine, "CYPHER 2.3 EXPLAIN MATCH n RETURN n")
    }
  }

  private val queryThatCannotRunWithCostPlanner = "MATCH (n:Movie) SET n.title = 'The Movie'"
  private val querySupportedByCostButNotCompiledRuntime = "MATCH (n:Movie)--(b), (a:A)--(c:C)--(d:D) RETURN count(*)"

  test("should not fail if cypher allowed to choose planner or we specify RULE for update query") {
    runWithConfig("dbms.cypher.hints.error" -> "true") {
      engine =>
        engine.execute(queryThatCannotRunWithCostPlanner)
        engine.execute(s"CYPHER planner=RULE $queryThatCannotRunWithCostPlanner")
        shouldHaveNoWarnings(engine.execute(s"EXPLAIN CYPHER planner=RULE $queryThatCannotRunWithCostPlanner"))
    }
  }

  test("should fail if asked to execute query with COST instead of falling back to RULE if hint errors turned on") {
    runWithConfig("dbms.cypher.hints.error" -> "true") {
      engine =>
        intercept[InvalidArgumentException](engine.execute(s"EXPLAIN CYPHER planner=COST $queryThatCannotRunWithCostPlanner"))
    }
  }

  test("should not fail if asked to execute query with COST and instead fallback to RULE and return a warning if hint errors turned off") {
    runWithConfig("dbms.cypher.hints.error" -> "false") {
      engine =>
        shouldHaveWarning(engine.execute(s"EXPLAIN CYPHER planner=COST $queryThatCannotRunWithCostPlanner"), Status.Statement.PlannerUnsupportedWarning)
    }
  }

  test("should not fail if asked to execute query with COST and instead fallback to RULE and return a warning by default") {
    runWithConfig() {
      engine =>
        shouldHaveWarning(engine.execute(s"EXPLAIN CYPHER planner=COST $queryThatCannotRunWithCostPlanner"), Status.Statement.PlannerUnsupportedWarning)
    }
  }

  test("should not fail nor generate a warning if asked to execute query without specifying runtime, knowing that compiled is default but will fallback silently to interpreted") {
    runWithConfig() {
      engine =>
        shouldHaveNoWarnings(engine.execute(s"EXPLAIN $querySupportedByCostButNotCompiledRuntime"))
    }
  }

  test("should not support old 2.0 and 2.1 compilers") {
    runWithConfig() {
      engine =>
        intercept[SyntaxException](engine.execute("CYPHER 2.0 MATCH n RETURN n"))
        intercept[SyntaxException](engine.execute("CYPHER 2.1 MATCH n RETURN n"))
    }
  }

  private def assertProfiled(engine: ExecutionEngine, q: String) {
    val result = engine.execute(q)
    val ignored = result.toList
    assert(result.executionPlanDescription().asJava.hasProfilerStatistics, s"$q was not profiled as expected")
    assert(result.planDescriptionRequested, s"$q was not flagged for planDescription")
  }

  private def assertExplained(engine: ExecutionEngine, q: String) {
    val result = engine.execute(q)
    val ignored = result.toList
    assert(!result.executionPlanDescription().asJava.hasProfilerStatistics, s"$q was not profiled as expected")
    assert(result.planDescriptionRequested, s"$q was not flagged for planDescription")
  }
}
