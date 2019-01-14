/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.v3_4.spi.CostBasedPlannerName
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.{ExecutionPlanDescription, GraphDatabaseService, QueryExecutionException, Result}
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.immutable.Map

class ExecutionEngineIT extends CypherFunSuite with GraphIcing {

  private var db : GraphDatabaseService = _

  override protected def stopTest(): Unit = {
    super.stopTest()
    if (db != null) {
      db.shutdown()
    }
  }

  test("by default when using cypher 2.3 some queries should default to COST") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan1 = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan1.getArguments.get("planner") should equal("COST")
    plan1.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
    plan2.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
  }

  test("by default when using cypher 3.1 some queries should default to COST") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.1").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan1 = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan1.getArguments.get("planner") should equal("COST")
    plan1.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
    plan2.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
  }

  test("by default when using cypher 3.4 some queries should default to COST") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.4").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan1 = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan1.getArguments.get("planner") should equal("COST")
    plan1.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
    plan2.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
  }

  test("should be able to set RULE as default when using cypher 2.3") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "RULE")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("RULE")
    plan.getArguments.get("planner-impl") should equal("RULE")
  }

  test("should be able to set RULE as default when using cypher 3.1") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "RULE")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.1").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("RULE")
    plan.getArguments.get("planner-impl") should equal("RULE")
  }

  test("should be able to force COST as default when using cypher 2.3") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "COST")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("COST")
    plan.getArguments.get("planner-impl") should equal("IDP")
  }

  test("should be able to force COST as default when using cypher 3.1") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "COST")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.1").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("COST")
    plan.getArguments.get("planner-impl") should equal("IDP")
  }

  test("should be able to force COST as default when using cypher 3.4") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "COST")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.4").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("COST")
    plan.getArguments.get("planner-impl") should equal("IDP")
  }

  test("should work if query cache size is set to zero") {
    //given
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.query_cache_size, "0").newGraphDatabase()

    // when
    db.execute("RETURN 42").close()

    // then no exception is thrown
  }

  test("should not refer to stale plan context in the cached execution plans") {
    // given
    db = new TestGraphDatabaseFactory().newImpermanentDatabase()

    // when
    db.execute("EXPLAIN MERGE (a:A) ON MATCH SET a.prop = 21  RETURN *").close()
    db.execute("EXPLAIN    MERGE (a:A) ON MATCH SET a.prop = 42 RETURN *").close()
  }

  test("should crash of erroneous parameters values if they are used") {
    // given
    db = new TestGraphDatabaseFactory().newImpermanentDatabase()

    // when
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("erroneous", ErroneousParameterValue())

    val result = db.execute("RETURN $erroneous AS x", params)

    // then
    intercept[QueryExecutionException] {
      result.columnAs[Int]("x").next()
    }
  }

  test("should ignore erroneous parameters values if they are not used") {
    // given
    db = new TestGraphDatabaseFactory().newImpermanentDatabase()

    // when
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("valid", Integer.valueOf(42))
    params.put("erroneous", ErroneousParameterValue())

    val result = db.execute("RETURN $valid AS x", params)

    // then
    result.columnAs[Int]("x").next() should equal(42)
  }

  private implicit class RichDb(db: GraphDatabaseCypherService) {
    def planDescriptionForQuery(query: String): ExecutionPlanDescription = {
      val res = db.execute(query)
      res.resultAsString()
      res.getExecutionPlanDescription
    }
  }

  implicit class RichExecutionEngine(engine: ExecutionEngine) {
    def profile(query: String, params: Map[String, Any]): Result =
      engine.profile(query, params, engine.queryService.transactionalContext(query = query -> params))

    def execute(query: String, params: Map[String, Any]): Result =
      engine.execute(query, params, engine.queryService.transactionalContext(query = query -> params))
  }

  private case class ErroneousParameterValue()
}
