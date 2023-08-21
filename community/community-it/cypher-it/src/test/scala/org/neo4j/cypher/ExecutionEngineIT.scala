/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.test.TestDatabaseManagementServiceBuilder

class ExecutionEngineIT extends CypherFunSuite with GraphIcing {

  private var db: GraphDatabaseService = _
  private var managementService: DatabaseManagementService = _

  override protected def afterEach(): Unit = {
    super.afterEach()
    if (db != null) {
      managementService.shutdown()
    }
  }

  test(s"by default when using cypher some queries should default to COST") {
    // given
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    db = managementService.database(DEFAULT_DATABASE_NAME)
    val service = new GraphDatabaseCypherService(db)

    // when
    val plan1 = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    // then
    plan1.getArguments.get("planner") should equal("COST")
    plan1.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
    plan2.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
  }

  test(s"should be able to force COST as default when using cypher") {
    // given
    managementService = new TestDatabaseManagementServiceBuilder()
      .impermanent()
      .setConfig(GraphDatabaseSettings.cypher_planner, GraphDatabaseSettings.CypherPlanner.COST).build
    db = managementService.database(DEFAULT_DATABASE_NAME)
    val service = new GraphDatabaseCypherService(db)

    // when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    // then
    plan.getArguments.get("planner") should equal("COST")
    plan.getArguments.get("planner-impl") should equal("IDP")
  }

  test("should work if query cache size is set to zero") {
    // given
    managementService = new TestDatabaseManagementServiceBuilder()
      .impermanent()
      .setConfig(GraphDatabaseSettings.query_cache_size, Integer.valueOf(0)).build()
    db = managementService.database(DEFAULT_DATABASE_NAME)

    // when
    val transaction = db.beginTx()
    try {
      transaction.execute("RETURN 42").close()
    } finally {
      transaction.close()
    }
    // then no exception is thrown
  }

  test("should not refer to stale plan context in the cached execution plans") {
    // given
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    db = managementService.database(DEFAULT_DATABASE_NAME)

    // when
    val transaction = db.beginTx()
    try {
      transaction.execute("EXPLAIN MERGE (a:A) ON MATCH SET a.prop = 21  RETURN *").close()
      transaction.execute("EXPLAIN    MERGE (a:A) ON MATCH SET a.prop = 42 RETURN *").close()
    } finally {
      transaction.close()
    }
  }

  test("should crash of erroneous parameters values if they are used") {
    // given
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    db = managementService.database(DEFAULT_DATABASE_NAME)

    // when
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("erroneous", ErroneousParameterValue())

    val transaction = db.beginTx()
    try {
      val result = transaction.execute("RETURN $erroneous AS x", params)

      // then
      intercept[QueryExecutionException] {
        result.columnAs[Int]("x").next()
      }
    } finally {
      transaction.close()
    }
  }

  test("should ignore erroneous parameters values if they are not used") {
    // given
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    db = managementService.database(DEFAULT_DATABASE_NAME)

    // when
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("valid", Integer.valueOf(42))
    params.put("erroneous", ErroneousParameterValue())

    val transaction = db.beginTx()
    try {
      val result = transaction.execute("RETURN $valid AS x", params)
      // then
      result.columnAs[Int]("x").next() should equal(42)
    } finally {
      transaction.close()
    }
  }

  implicit private class RichDb(db: GraphDatabaseCypherService) {

    def planDescriptionForQuery(query: String): ExecutionPlanDescription = {
      db.withTx(tx => {
        val res = tx.execute(query)
        try {
          res.resultAsString()
          res.getExecutionPlanDescription
        } finally {
          res.close()
        }
      })
    }
  }

  private case class ErroneousParameterValue()
}
