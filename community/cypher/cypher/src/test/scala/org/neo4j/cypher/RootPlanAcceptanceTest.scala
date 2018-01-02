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

import org.neo4j.cypher.internal.compiler.v2_3._

class RootPlanAcceptanceTest extends ExecutionEngineFunSuite {

  test("should include version information in root plan description for queries of each legacy version") {

    //v2.2 and v2.3 must be handled separately since the resulting compiler is dependent on query
    val versions = CypherVersion.all
      .filter(v => !v.name.startsWith(CypherVersion.v2_3.name) && !v.name.startsWith(CypherVersion.v2_2.name))
    versions.foreach { v =>
      given("create() return 1")
        .withCypherVersion(v)
        .shouldHaveCypherVersion(v)
    }
  }

  test("cost should be default planner in 2.3") {
    given("match n return n")
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("interpreted should be default runtime in 2.3") {
    given("match n return n")
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHaveRuntime(InterpretedRuntimeName)
  }

  test("should use cost for varlength in 2.3") {
    given("match (a)-[r:T1*]->(b) return a,r,b")
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHaveCypherVersion(CypherVersion.v2_3)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("should use cost for cycles in 2.3") {
    given("match (a)-[r]->(a) return a")
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHaveCypherVersion(CypherVersion.v2_3)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("should fallback to Rule for updates in 2.3") {
    given("create() return 1")
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHaveCypherVersion(CypherVersion.v2_3)
      .shouldHavePlanner(RulePlannerName)
  }

  test("should report RULE if we ask it for UNION queries") {
    given(
      """MATCH p=(n:Person {first_name: 'Shawna'})-[:FRIEND_OF]-(m:Person)
        |RETURN p UNION MATCH p=(n:Person {first_name: 'Shawna'})-[:FRIEND_OF]-()-[:FRIEND_OF]-(m:Person) RETURN p"""
        .stripMargin)
      .withCypherVersion(CypherVersion.v2_3)
      .withPlanner(RulePlannerName)
      .shouldHaveCypherVersion(CypherVersion.v2_3)
      .shouldHavePlanner(RulePlannerName)
  }

  test("troublesome query that should be run in cost") {
    given(
      """MATCH (person)-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-()-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(coc)-[:DIRECTED]->()
        |WHERE NOT ((coc)-[:ACTED_IN]->()<-[:ACTED_IN]-(person)) AND coc <> person
        |RETURN coc, COUNT(*) AS times
        |ORDER BY times DESC
        |LIMIT 10""".stripMargin)
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHaveCypherVersion(CypherVersion.v2_3)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("another troublesome query that should be run in cost") {
    given(
      """MATCH (s:Location {name:'DeliverySegment-257227'}), (e:Location {name:'DeliverySegment-476821'})
        |MATCH (s)<-[:DELIVERY_ROUTE]-(db1) MATCH (db2)-[:DELIVERY_ROUTE]->(e)
        |MATCH (db1)<-[:CONNECTED_TO]-()-[:CONNECTED_TO]-(db2) RETURN s""".stripMargin)
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHaveCypherVersion(CypherVersion.v2_3)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("query that does not go through the compiled runtime") {
    given("MATCH n RETURN n, count(*)")
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHaveCypherVersion(CypherVersion.v2_3)
      .shouldHaveRuntime(InterpretedRuntimeName)
  }

  test("query that does not go through the compiled runtime nor Cost") {
    given(
      "CREATE ()")
      .withCypherVersion(CypherVersion.v2_3)
      .shouldHaveCypherVersion(CypherVersion.v2_3)
      .shouldHaveRuntime(InterpretedRuntimeName)
      .shouldHavePlanner(RulePlannerName)
  }

  test("AllNodesScan should be the only child of the plan") {
    val description = given("match n return n").planDescription
    var children = description.getChildren
    children should have size 1
    while (children.get(0).getChildren.size() > 0) {
      children = children.get(0).getChildren
      children should have size 1
    }

    children.get(0).getName should be("AllNodesScan")
  }


  test("DbHits should contain proper values in interpreted runtime") {
    val description = given("match n return n")
      .withRuntime(InterpretedRuntimeName)
      .planDescription
    val children = description.getChildren
    children should have size 1
    description.getArguments.get("DbHits") should equal(0) // ProduceResults has no hits
    children.get(0).getArguments.get("DbHits") should equal(1) // AllNodesScan has 1 hit
  }

  test("Rows should be properly formatted in interpreted runtime") {
    given("match n return n")
      .withRuntime(InterpretedRuntimeName)
      .planDescription.getArguments.get("Rows") should equal(0)
  }

  test("EstimatedRows should be properly formatted") {
    given("match n return n").planDescription.getArguments.get("EstimatedRows") should equal(0)
  }

  for(planner <- Seq(GreedyPlannerName, IDPPlannerName, DPPlannerName, RulePlannerName);
      runtime <- Seq(InterpretedRuntimeName)) {

    test(s"Should report correct planner and runtime used $planner + $runtime") {
      given("match n return n")
        .withPlanner(planner)
        .withRuntime(runtime)
        .shouldHaveCypherVersion(CypherVersion.v2_3)
        .shouldHavePlanner(planner)
        .shouldHaveRuntime(runtime)
    }
  }

  def given(query: String) = TestQuery(query)

  case class TestQuery(query: String,
                       cypherVersion: Option[CypherVersion] = None,
                       planner: Option[PlannerName] = None,
                       runtime: Option[RuntimeName] = None) {

    lazy val planDescription = execute()

    def withCypherVersion(version: CypherVersion) = copy(cypherVersion = Some(version))

    def withPlanner(planner: PlannerName) = copy(planner = Some(planner))

    def withRuntime(runtime: RuntimeName) = copy(runtime = Some(runtime))

    def shouldHaveCypherVersion(version: CypherVersion) = {
      planDescription.getArguments.get("version") should equal(s"CYPHER ${version.name}")
      this
    }

    def shouldHavePlanner(planner: PlannerName) = {
      planDescription.getArguments.get("planner") should equal(s"${planner.toTextOutput}")
      planDescription.getArguments.get("planner-impl") should equal(s"${planner.name}")
      this
    }

    def shouldHaveRuntime(runtime: RuntimeName) = {
      planDescription.getArguments.get("runtime") should equal(s"${runtime.toTextOutput}")
      planDescription.getArguments.get("runtime-impl") should equal(s"${runtime.name}")
      this
    }

    private def execute() = {
      val prepend = (cypherVersion, planner, runtime) match {
        case (None, None, None) => ""
        case _ =>
          val version = cypherVersion.map(_.name).getOrElse("")
          val plannerString = planner.map("planner=" + _.name).getOrElse("")
          val runtimeString = runtime.map("runtime=" + _.name).getOrElse("")
          s"CYPHER $version $plannerString $runtimeString"
      }
      val result = eengine.profile(s"$prepend $query")
      result.size
      val executionResult = result.executionPlanDescription()
      executionResult.asJava
    }
  }

}
