/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.api.exceptions.Status

import scala.collection.JavaConverters._

class CypherCompatibilityTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport {

  val QUERY = "MATCH (n:Label) RETURN n"

  override protected def initTest(): Unit = ()

  override protected def stopTest(): Unit = ()

  test("should match paths correctly with rule planner in compatiblity mode") {
    runWithConfig() {
      db =>
        graph = db
        relate(createNode(), createNode(), "T")
        val query = "MATCH (n)-[r:T]->(m) RETURN count(*)"
        db.execute(s"CYPHER 2.3 planner=rule $query").columnAs[Long]("count(*)").next() shouldBe 1
        db.execute(s"CYPHER 2.3 $query").columnAs[Long]("count(*)").next() shouldBe 1
        db.execute(s"CYPHER 3.1 planner=rule $query").columnAs[Long]("count(*)").next() shouldBe 1
        db.execute(s"CYPHER 3.1 $query").columnAs[Long]("count(*)").next() shouldBe 1
    }
  }

  test("should be able to switch between versions") {
    runWithConfig() {
      db =>
        db.execute(s"CYPHER 2.3 $QUERY").asScala.toList shouldBe empty
        db.execute(s"CYPHER 3.1 $QUERY").asScala.toList shouldBe empty
        db.execute(s"CYPHER 3.4 $QUERY").asScala.toList shouldBe empty
    }
  }

  test("should be able to switch between versions2") {
    runWithConfig() {
      db =>
        db.execute(s"CYPHER 3.1 $QUERY").asScala.toList shouldBe empty
        db.execute(s"CYPHER 2.3 $QUERY").asScala.toList shouldBe empty
    }
  }

  test("should be able to override config") {
    runWithConfig(GraphDatabaseSettings.cypher_parser_version -> "2.3") {
      db =>
        db.execute(s"CYPHER 3.1 $QUERY").asScala.toList shouldBe empty
    }
  }

  test("should be able to override config2") {
    runWithConfig(GraphDatabaseSettings.cypher_parser_version -> "3.1") {
      db =>
        db.execute(s"CYPHER 2.3 $QUERY").asScala.toList shouldBe empty
    }
  }

  test("should use default version by default") {
    runWithConfig() {
      db =>
        val result = db.execute(QUERY)
        result.asScala.toList shouldBe empty
        result.getExecutionPlanDescription.getArguments.get("version") should equal("CYPHER 3.4")
    }
  }

  test("should handle profile in compiled runtime") {
    runWithConfig() {
      db =>
        assertProfiled(db, "CYPHER 3.1 runtime=compiled PROFILE MATCH (n) RETURN n")
        assertProfiled(db, "CYPHER 3.4 runtime=compiled PROFILE MATCH (n) RETURN n")
    }
  }

  test("should handle profile in interpreted runtime") {
    runWithConfig() {
      engine =>
        assertProfiled(engine, "CYPHER 2.3 runtime=interpreted PROFILE MATCH (n) RETURN n")
        assertProfiled(engine, "CYPHER 3.1 runtime=interpreted PROFILE MATCH (n) RETURN n")
        assertProfiled(engine, "CYPHER 3.4 runtime=interpreted PROFILE MATCH (n) RETURN n")
    }
  }

  test("should allow the use of explain in the supported compilers") {
    runWithConfig() {
      engine =>
        assertExplained(engine, "CYPHER 2.3 EXPLAIN MATCH (n) RETURN n")
        assertExplained(engine, "CYPHER 3.1 EXPLAIN MATCH (n) RETURN n")
        assertExplained(engine, "CYPHER 3.4 EXPLAIN MATCH (n) RETURN n")
    }
  }

  private val querySupportedByCostButNotCompiledRuntime = "MATCH (n:Movie)--(b), (a:A)--(c:C)--(d:D) RETURN count(*)"

  test("should not fail if asked to execute query with runtime=compiled on simple query") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      db =>
        db.execute("MATCH (n:Movie) RETURN n")
        db.execute("CYPHER runtime=compiled MATCH (n:Movie) RETURN n")
        shouldHaveNoWarnings(db.execute("EXPLAIN CYPHER runtime=compiled MATCH (n:Movie) RETURN n"))
    }
  }

  test("should fail if asked to execute query with runtime=compiled instead of falling back to interpreted if hint errors turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      db =>
        intercept[QueryExecutionException](
          db.execute(s"EXPLAIN CYPHER runtime=compiled $querySupportedByCostButNotCompiledRuntime")
        ).getStatusCode should equal("Neo.ClientError.Statement.ArgumentError")
    }
  }

  test("should not fail if asked to execute query with runtime=compiled and instead fallback to interpreted and return a warning if hint errors turned off") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "false") {
      db =>
        val result = db.execute(s"EXPLAIN CYPHER runtime=compiled $querySupportedByCostButNotCompiledRuntime")
        shouldHaveWarning(result, Status.Statement.RuntimeUnsupportedWarning)
    }
  }

  test("should not fail if asked to execute query with runtime=compiled and instead fallback to interpreted and return a warning by default") {
    runWithConfig() {
      db =>
        val result = db.execute(s"EXPLAIN CYPHER runtime=compiled $querySupportedByCostButNotCompiledRuntime")
        shouldHaveWarning(result, Status.Statement.RuntimeUnsupportedWarning)
    }
  }

  test("should not fail nor generate a warning if asked to execute query without specifying runtime, knowing that compiled is default but will fallback silently to interpreted") {
    runWithConfig() {
      db =>
        shouldHaveNoWarnings(db.execute(s"EXPLAIN $querySupportedByCostButNotCompiledRuntime"))
    }
  }

  test("should not support old 1,9, 2.0, 2.1, and 2.2 compilers") {
    runWithConfig() {
      db =>
        intercept[QueryExecutionException](db.execute("CYPHER 1.9 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 2.0 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 2.1 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 2.2 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
    }
  }

  private def assertProfiled(db: GraphDatabaseCypherService, q: String) {
    val result = db.execute(q)
    result.resultAsString()
    assert(result.getExecutionPlanDescription.hasProfilerStatistics, s"$q was not profiled as expected")
    assert(result.getQueryExecutionType.requestedExecutionPlanDescription(), s"$q was not flagged for planDescription")
  }

  private def assertExplained(db: GraphDatabaseCypherService, q: String) {
    val result = db.execute(q)
    result.resultAsString()
    assert(!result.getExecutionPlanDescription.hasProfilerStatistics, s"$q was not explained as expected")
    assert(result.getQueryExecutionType.requestedExecutionPlanDescription(), s"$q was not flagged for planDescription")
  }
}
