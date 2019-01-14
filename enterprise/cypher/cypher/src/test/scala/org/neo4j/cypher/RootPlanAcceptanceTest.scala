/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{CompiledRuntimeName, InterpretedRuntimeName, RuntimeName, SlottedRuntimeName}
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.PlannerName
import org.neo4j.cypher.internal.planner.v3_4.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName}
import org.neo4j.graphdb.ExecutionPlanDescription

class RootPlanAcceptanceTest extends ExecutionEngineFunSuite {

  test("query that does not go through the compiled runtime") {
    given("MATCH (n) RETURN n, count(*)")
      .withCypherVersion(CypherVersion.v3_4)
      .shouldHaveCypherVersion(CypherVersion.v3_4)
      .shouldHaveRuntime(SlottedRuntimeName)
  }

  test("query that lacks support from the compiled runtime") {
    given("CREATE ()")
      .withCypherVersion(CypherVersion.v3_4)
      .withRuntime(CompiledRuntimeName)
      .shouldHaveCypherVersion(CypherVersion.v3_4)
      .shouldHaveRuntime(SlottedRuntimeName)
  }

  test("query that should go through the compiled runtime") {
    given("MATCH (a)-->(b) RETURN a")
      .withCypherVersion(CypherVersion.v3_4)
      .withRuntime(CompiledRuntimeName)
      .shouldHaveCypherVersion(CypherVersion.v3_4)
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
        .shouldHaveCypherVersion(CypherVersion.v3_4)
        .shouldHavePlanner(planner)
        .shouldHaveRuntime(runtime)
    }
  }

  test("should show_java_source") {
    val res = eengine.execute("CYPHER debug=generate_java_source debug=show_java_source MATCH (n) RETURN n", Map.empty[String, Object])
    res.resultAsString()
    shouldContainSourceCode(res.getExecutionPlanDescription)
  }

  test("should show_bytecode") {
    val res = eengine.execute("CYPHER debug=show_bytecode MATCH (n) RETURN n", Map.empty[String, Object])
    res.resultAsString()
    shouldContainByteCode(res.getExecutionPlanDescription)
  }

  test("should show_java_source and show_bytecode") {
    val res = eengine.execute("CYPHER debug=generate_java_source debug=show_java_source debug=show_bytecode MATCH (n) RETURN n", Map.empty[String, Object])
    res.resultAsString()
    shouldContainSourceCode(res.getExecutionPlanDescription)
    shouldContainByteCode(res.getExecutionPlanDescription)
  }

  private def shouldContainSourceCode(planDescription: ExecutionPlanDescription) = {
    shouldContain("source", planDescription)
  }

  private def shouldContainByteCode(planDescription: ExecutionPlanDescription) = {
    shouldContain("bytecode", planDescription)
  }

  import scala.collection.JavaConverters._

  private def shouldContain(argument:String, planDescription: ExecutionPlanDescription) = {
    if(!planDescription.getArguments.asScala.exists {
      case (name: String, code: String) if name.startsWith(s"$argument:") =>
        !code.isEmpty
      case _ => false
    }) {
      fail(s"no $argument present: $planDescription")
    }
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
      result.resultAsString()
      val executionResult = result.getExecutionPlanDescription
      executionResult
    }
  }
}
