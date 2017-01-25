/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.javacompat.PlanDescription

class RootPlanAcceptanceTest extends ExecutionEngineFunSuite {

  test("query that does not go through the compiled runtime") {
    given("MATCH (n) RETURN n, count(*)")
      .withCypherVersion(CypherVersion.v3_2)
      .shouldHaveCypherVersion(CypherVersion.v3_2)
      .shouldHaveRuntime(InterpretedRuntimeName)
  }

  test("query that lacks support from the compiled runtime") {
    given("CREATE ()")
      .withCypherVersion(CypherVersion.v3_2)
      .withRuntime(CompiledRuntimeName)
      .shouldHaveCypherVersion(CypherVersion.v3_2)
      .shouldHaveRuntime(InterpretedRuntimeName)
  }

  test("query that should go through the compiled runtime") {
    given("MATCH (a)-->(b) RETURN a")
      .withCypherVersion(CypherVersion.v3_2)
      .withRuntime(CompiledRuntimeName)
      .shouldHaveCypherVersion(CypherVersion.v3_2)
      .shouldHaveRuntime(CompiledRuntimeName)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("DbHits should contain proper values in compiled runtime") {
    val description = given("match (n) return n")
      .withRuntime(CompiledRuntimeName)
      .planDescription
    val children = description.getChildren
    children should have size 1
    description.getArguments.get("DbHits") should equal(0) // ProduceResults has no hits
    children.get(0).getArguments.get("DbHits") should equal(1) // AllNodesScan has 1 hit
  }

  test("Rows should be properly formatted in compiled runtime") {
    given("match (n) return n")
      .withRuntime(CompiledRuntimeName)
      .planDescription.getArguments.get("Rows") should equal(0)
  }

  for(planner <- Seq(IDPPlannerName, DPPlannerName);
      runtime <- Seq(CompiledRuntimeName, InterpretedRuntimeName)) {

    test(s"Should report correct planner and runtime used $planner + $runtime") {
      given("match (n) return n")
        .withPlanner(planner)
        .withRuntime(runtime)
        .shouldHaveCypherVersion(CypherVersion.v3_2)
        .shouldHavePlanner(planner)
        .shouldHaveRuntime(runtime)
    }
  }

  def given(query: String) = TestQuery(query)

  case class TestQuery(query: String,
                       cypherVersion: Option[CypherVersion] = None,
                       planner: Option[PlannerName] = None,
                       runtime: Option[RuntimeName] = None) {

    lazy val planDescription: PlanDescription = execute()

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

    private def execute() = {
      val prepend = (cypherVersion, planner, runtime) match {
        case (None, None, None) => ""
        case _ =>
          val version = cypherVersion.map(_.name).getOrElse("")
          val plannerString = planner.map("planner=" + _.name).getOrElse("")
          val runtimeString = runtime.map("runtime=" + _.name).getOrElse("")
          s"CYPHER $version $plannerString $runtimeString"
      }
      val result = eengine.profile(s"$prepend $query", Map.empty[String, Object])
      result.size
      val executionResult = result.executionPlanDescription()
      executionResult.asJava
    }
  }
}
