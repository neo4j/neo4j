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

import org.neo4j.cypher.internal.compiler.v2_3.CostBasedPlannerName
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.test.TestGraphDatabaseFactory

class ExecutionEngineIT extends CypherFunSuite {

  test("by default when using cypher 2.2 some queries should default to COST") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.2").newGraphDatabase()

    //when
    val plan1 = db.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = db.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan1.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner") should equal("COST")
  }

  test("by default when using cypher 2.3 some queries should default to COST") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()

    //when
    val plan1 = db.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = db.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan1.getArguments.get("planner") should equal("COST")
    plan1.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
    plan2.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
  }

  test("should be able to set RULE as default when using cypher 2.2") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "RULE")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.2").newGraphDatabase()

    //when
    val plan = db.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("RULE")
  }

  test("should be able to set RULE as default when using cypher 2.3") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "RULE")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()

    //when
    val plan = db.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("RULE")
    plan.getArguments.get("planner-impl") should equal("RULE")
  }

  test("should be able to force COST as default when using cypher 2.2") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "COST")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.2").newGraphDatabase()

    //when
    val plan = db.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("COST")
  }


  test("should be able to force COST as default when using cypher 2.3") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "COST")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()

    //when
    val plan = db.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("COST")
    plan.getArguments.get("planner-impl") should equal("IDP")
  }

  test("should work if query cache size is set to zero") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.query_cache_size, "0").newGraphDatabase()

    // when
    db.execute("RETURN 42").close()

    // then no exception is thrown
  }

  test("should not leak transaction when closing the result for a query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val engine = new ExecutionEngine(db)

    // when
    db.execute("return 1").close()
    // then
    txBridge(db).hasTransaction shouldBe false

    // when
    engine.execute("return 1").close()
    // then
    txBridge(db).hasTransaction shouldBe false

    // when
    engine.execute("return 1").javaIterator.close()
    // then
    txBridge(db).hasTransaction shouldBe false
  }

  test("should not leak transaction when closing the result for a profile query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val engine = new ExecutionEngine(db)

    // when
    db.execute("profile return 1").close()
    // then
    txBridge(db).hasTransaction shouldBe false

    // when
    engine.execute("profile return 1").close()
    // then
    txBridge(db).hasTransaction shouldBe false

    // when
    engine.execute("profile return 1").javaIterator.close()
    // then
    txBridge(db).hasTransaction shouldBe false

    // when
    engine.profile("return 1").close()
    // then
    txBridge(db).hasTransaction shouldBe false

    // when
    engine.profile("return 1").javaIterator.close()
    // then
    txBridge(db).hasTransaction shouldBe false
  }

  test("should not leak transaction when closing the result for an explain query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val engine = new ExecutionEngine(db)

    // when
    db.execute("explain return 1").close()
    // then
    txBridge(db).hasTransaction shouldBe false

    // when
    engine.execute("explain return 1").close()
    // then
    txBridge(db).hasTransaction shouldBe false

    // when
    engine.execute("explain return 1").javaIterator.close()
    // then
    txBridge(db).hasTransaction shouldBe false
  }

  private implicit class RichDb(db: GraphDatabaseService) {
    def planDescriptionForQuery(query: String) = {
      val res = db.execute(query)
      res.resultAsString()
      res.getExecutionPlanDescription
    }
  }

  private def txBridge(db: GraphDatabaseService) = {
    db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  }
}
