/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.test.ImpermanentGraphDatabase

import scala.collection.JavaConverters._

class CypherCompatibilityTest extends CypherFunSuite {

  val QUERY_2_0_ONLY = "MATCH (n:Label) RETURN n"
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
        engine.execute(s"CYPHER 2.0 $QUERY_FOR_BOTH").toList shouldBe empty
    }
  }

  test("should_be_able_to_switch_between_versions2") {
    runWithConfig() {
      engine =>
        engine.execute(s"CYPHER 2.0 $QUERY_FOR_BOTH").toList shouldBe empty
        engine.execute(s"CYPHER 1.9 $QUERY_FOR_BOTH") shouldBe empty
    }
  }

  test("should_be_able_to_override_config") {
    runWithConfig("cypher_parser_version" -> "2.0") {
      engine =>
        engine.execute(s"CYPHER 1.9 $QUERY_1_9_ONLY").toList shouldBe empty
    }
  }

  test("should_be_able_to_override_config2") {
    runWithConfig("cypher_parser_version" -> "1.9") {
      engine =>
        engine.execute(s"CYPHER 2.0 $QUERY_2_0_ONLY").toList shouldBe empty
    }
  }

  test("should_use_default_version_by_default") {
    runWithConfig() {
      engine =>
        engine.execute(QUERY_2_0_ONLY).toList shouldBe empty
    }
  }

  test("should handle profile with older versions of the compiler") {
    runWithConfig() {
      (engine: ExecutionEngine) =>
        assertProfiled(engine, "CYPHER 1.9 PROFILE START n=node(*) RETURN n")
        assertProfiled(engine, "CYPHER 2.0 PROFILE MATCH n RETURN n")
        assertProfiled(engine, "CYPHER 2.1 PROFILE MATCH n RETURN n")
    }
  }

  test("should not allow EXPLAIN to be used with older compilers") {
    runWithConfig() {
      engine =>
        intercept[InvalidArgumentException](engine.execute("CYPHER 1.9 EXPLAIN MATCH n RETURN n"))
        intercept[InvalidArgumentException](engine.execute("CYPHER 2.0 EXPLAIN MATCH n RETURN n"))
        intercept[InvalidArgumentException](engine.execute("CYPHER 2.1 EXPLAIN MATCH n RETURN n"))
    }
  }

  test("should fail if asked to execute query with COST instead to fallback to RULE") {
    runWithConfig() {
      engine =>
        engine.execute("MATCH (n:Movie) SET n.title = 'The Movie'")
        engine.execute("PLANNER RULE MATCH (n:Movie) SET n.title = 'The Movie'")
        intercept[InvalidArgumentException](engine.execute("PLANNER COST MATCH (n:Movie) SET n.title = 'The Movie'"))
    }
  }

  private def assertProfiled(engine: ExecutionEngine, q: String) {
    val result = engine.execute(q)
    val ignored = result.toList
    assert(result.executionPlanDescription().asJava.hasProfilerStatistics, s"$q was not profiled as expected")
    assert(result.planDescriptionRequested, s"$q was not flagged for planDescription")
  }

  private def runWithConfig(m: (String, String)*)(run: ExecutionEngine => Unit) = {
    val config: util.Map[String, String] = m.toMap.asJava

    val graph = new ImpermanentGraphDatabase(config) with Snitch
    try {
      val engine = new ExecutionEngine(graph)
      run(engine)
    } finally {
      graph.shutdown()
    }

  }
}
