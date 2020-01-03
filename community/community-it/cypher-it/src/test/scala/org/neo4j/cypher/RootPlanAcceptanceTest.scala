/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.{InterpretedRuntimeName, RuntimeName}
import org.neo4j.graphdb.ExecutionPlanDescription

class RootPlanAcceptanceTest extends ExecutionEngineFunSuite {

  test("cost should be default planner in 4.0") {
    given("match (n) return n")
      .withCypherVersion(CypherVersion.v4_0)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("3.5 query should have 3.5 version") {
    given("match (n) return n")
      .withCypherVersion(CypherVersion.v3_5)
      .shouldHaveCypherVersion(CypherVersion.v3_5)
  }

  test("interpreted should be default runtime in 4.0") {
    given("match (n) return n")
      .withCypherVersion(CypherVersion.v4_0)
      .shouldHaveRuntime(InterpretedRuntimeName)
  }

  test("should use cost for varlength in 4.0") {
    given("match (a)-[r:T1*]->(b) return a,r,b")
      .withCypherVersion(CypherVersion.v4_0)
      .shouldHaveCypherVersion(CypherVersion.v4_0)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("should use cost for cycles in 4.0") {
    given("match (a)-[r]->(a) return a")
      .withCypherVersion(CypherVersion.v4_0)
      .shouldHaveCypherVersion(CypherVersion.v4_0)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("should handle updates in 4.0") {
    given("create() return 1")
      .withCypherVersion(CypherVersion.v4_0)
      .shouldHaveCypherVersion(CypherVersion.v4_0)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("troublesome query that should be run in cost") {
    given(
      """MATCH (person)-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-()-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(coc)-[:DIRECTED]->()
        |WHERE NOT ((coc)-[:ACTED_IN]->()<-[:ACTED_IN]-(person)) AND coc <> person
        |RETURN coc, COUNT(*) AS times
        |ORDER BY times DESC
        |LIMIT 10""".stripMargin)
      .withCypherVersion(CypherVersion.v4_0)
      .shouldHaveCypherVersion(CypherVersion.v4_0)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("another troublesome query that should be run in cost") {
    given(
      """MATCH (s:Location {name:'DeliverySegment-257227'}), (e:Location {name:'DeliverySegment-476821'})
        |MATCH (s)<-[:DELIVERY_ROUTE]-(db1) MATCH (db2)-[:DELIVERY_ROUTE]->(e)
        |MATCH (db1)<-[:CONNECTED_TO]-()-[:CONNECTED_TO]-(db2) RETURN s""".stripMargin)
      .withCypherVersion(CypherVersion.v4_0)
      .shouldHaveCypherVersion(CypherVersion.v4_0)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("AllNodesScan should be the only child of the plan") {
    val description = given("match (n) return n").planDescription
    var children = description.getChildren
    children should have size 1
    while (children.get(0).getChildren.size() > 0) {
      children = children.get(0).getChildren
      children should have size 1
    }

    children.get(0).getName should be("AllNodesScan")
  }

  test("DbHits should contain proper values in interpreted runtime") {
    val description = given("match (n) return n")
      .withRuntime(InterpretedRuntimeName)
      .planDescription
    val children = description.getChildren
    children should have size 1
    description.getArguments.get("DbHits") should equal(0) // ProduceResults has no hits
    children.get(0).getArguments.get("DbHits") should equal(1) // AllNodesScan has 1 hit
  }

  test("Rows should be properly formatted in interpreted runtime") {
    given("match (n) return n")
      .withRuntime(InterpretedRuntimeName)
      .planDescription.getArguments.get("Rows") should equal(0)
  }

  test("EstimatedRows should be properly formatted") {
    // on missing statistics, we fake cardinality to 10
    given("MATCH (n) RETURN n").planDescription.getArguments.get("EstimatedRows") should equal(10.0)
  }

  def given(query: String) = TestQuery(query)

  case class TestQuery(query: String,
                       cypherVersion: Option[CypherVersion] = None,
                       planner: Option[PlannerName] = None,
                       runtime: Option[RuntimeName] = None) {

    lazy val planDescription: ExecutionPlanDescription = execute()

    def withCypherVersion(version: CypherVersion): TestQuery = copy(cypherVersion = Some(version))

    def withPlanner(planner: PlannerName): TestQuery = copy(planner = Some(planner))

    def withRuntime(runtime: RuntimeName): TestQuery = copy(runtime = Some(runtime))

    def shouldHaveCypherVersion(version: CypherVersion): TestQuery = {
      planDescription.getArguments.get("version") should equal(s"CYPHER ${version.name}")
      this
    }

    def shouldHavePlanner(planner: PlannerName): TestQuery = {
      planDescription.getArguments.get("planner") should equal(s"${planner.toTextOutput}")
      planDescription.getArguments.get("planner-impl") should equal(s"${planner.name}")
      this
    }

    def shouldHaveRuntime(runtime: RuntimeName): TestQuery = {
      planDescription.getArguments.get("runtime") should equal(s"${runtime.toTextOutput}")
      planDescription.getArguments.get("runtime-impl") should equal(s"${runtime.name}")
      this
    }

    private def execute(): ExecutionPlanDescription = {
      graph.withTx { tx =>
        val prepend = (cypherVersion, planner, runtime) match {
          case (None, None, None) => ""
          case _ =>
            val version = cypherVersion.map(_.name).getOrElse("")
            val plannerString = planner.map("planner=" + _.name).getOrElse("")
            val runtimeString = runtime.map("runtime=" + _.name).getOrElse("")
            s"CYPHER $version $plannerString $runtimeString"
        }
        val result = executeOfficial( tx, s"$prepend PROFILE $query")
        result.resultAsString()
        result.getExecutionPlanDescription()
      }
    }
  }
}
