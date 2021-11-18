/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.config.Setting

import scala.collection.JavaConverters.mapAsJavaMapConverter

class PreParserTest extends CypherFunSuite {

  private def preParserWith(settings: (Setting[_], AnyRef)*) = new PreParser(
    CypherConfiguration.fromConfig(Config.defaults(settings.toMap.asJava)),
    new LFUCache[String, PreParsedQuery](TestExecutorCaffeineCacheFactory, 0))

  private val preParser = preParserWith()

  test("should not allow inconsistent runtime options") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER runtime=slotted runtime=interpreted RETURN 42"))
  }

  test("should not allow multiple versions") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER 3.5 CYPHER 4.3 RETURN 42"))
  }

  test("should not allow both EXPLAIN and PROFILE") {
    intercept[InvalidArgumentException](preParser.preParseQuery("EXPLAIN PROFILE RETURN 42"))
    intercept[InvalidArgumentException](preParser.preParseQuery("PROFILE EXPLAIN RETURN 42"))
  }

  test("should not allow unknown options") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER foo=idp RETURN 42"))
      .getMessage should include("foo")
  }

  test("should not allow unknown debug flags") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER debug=idp RETURN 42"))
      .getMessage should include("idp")
  }

  test("should accept just one operator execution mode") {
    preParser.preParseQuery("CYPHER operatorEngine=interpreted RETURN 42").options.queryOptions.operatorEngine should equal(CypherOperatorEngineOption.interpreted)
  }

  test("should accept just one replan strategy") {
    preParser.preParseQuery("CYPHER replan=force RETURN 42").options.queryOptions.replan should equal(CypherReplanOption.force)
    preParser.preParseQuery("CYPHER replan=skip RETURN 42").options.queryOptions.replan should equal(CypherReplanOption.skip)
    preParser.preParseQuery("CYPHER replan=default RETURN 42").options.queryOptions.replan should equal(CypherReplanOption.default)
  }

  test("should accept just one connect components planner") {
    preParser.preParseQuery("CYPHER connectComponentsPlanner=idp RETURN 42").options.queryOptions.connectComponentsPlanner should equal(CypherConnectComponentsPlannerOption.idp)
  }

  test("should not allow multiple conflicting replan strategies") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER replan=force replan=skip RETURN 42"))
  }

  test("should accept just one interpreted pipes fallback mode") {
    preParser.preParseQuery("CYPHER interpretedPipesFallback=disabled RETURN 42").options.queryOptions.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.disabled)
    preParser.preParseQuery("CYPHER interpretedPipesFallback=default RETURN 42").options.queryOptions.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.default)
    preParser.preParseQuery("CYPHER interpretedPipesFallback=whitelisted_plans_only RETURN 42").options.queryOptions.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.whitelistedPlansOnly)
    preParser.preParseQuery("CYPHER interpretedPipesFallback=all RETURN 42").options.queryOptions.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.allPossiblePlans)
  }

  test("should not allow multiple conflicting interpreted pipes fallback modes") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER interpretedPipesFallback=all interpretedPipesFallback=disabled RETURN 42"))
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER interpretedPipesFallback=default interpretedPipesFallback=disabled RETURN 42"))
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER interpretedPipesFallback=default interpretedPipesFallback=all RETURN 42"))
  }

  test("should only allow interpreted pipes fallback mode in pipelined runtime") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER runtime=slotted interpretedPipesFallback=all RETURN 42"))
  }

  test("should not allow multiple conflicting connect component planners") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER connectComponentsPlanner=idp connectComponentsPlanner=greedy RETURN 42"))
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER connectComponentsPlanner=greedy connectComponentsPlanner=idp RETURN 42"))
  }

  test("should parse all variants of periodic commit") {
    val variants =
      List(
        "USING PERIODIC COMMIT",
        " USING PERIODIC COMMIT",
        "USING  PERIODIC COMMIT",
        "USING PERIODIC  COMMIT",
        """USING
           PERIODIC
           COMMIT""",
        "CYPHER 3.5 planner=cost debug=tostring USING PERIODIC COMMIT",
        "CYPHER 4.3 planner=cost debug=tostring USING PERIODIC COMMIT",
        "CYPHER 4.4 planner=cost debug=tostring USING PERIODIC COMMIT",
        "using periodic commit",
        "UsING pERIOdIC COMmIT"
      )

    for (x <- variants) {
      val query = " LOAD CSV file://input.csv AS row CREATE (n)"
      preParser.preParseQuery(x+query).options.isPeriodicCommit should be(true)
    }
  }

  test("should not call periodic commit on innocent (but evil) queries") {
    val queries =
      List(
        "MATCH (n) RETURN n",
        "CREATE ({name: 'USING PERIODIC COMMIT'})",
        "CREATE ({`USING PERIODIC COMMIT`: true})",
        "CREATE (:`USING PERIODIC COMMIT`)",
        "CYPHER 3.5 debug=tostring PROFILE CREATE ({name: 'USING PERIODIC COMMIT'})",
        "CYPHER 4.3 debug=tostring PROFILE CREATE ({name: 'USING PERIODIC COMMIT'})",
        "CYPHER 4.4 debug=tostring PROFILE CREATE ({name: 'USING PERIODIC COMMIT'})",
        """CREATE ({name: '
          |USING PERIODIC COMMIT')""".stripMargin
      )

    for (query <- queries) {
      preParser.preParseQuery(query).options.isPeriodicCommit should be(false)
    }
  }

  test("should take defaults from config") {
    preParserWith(GraphDatabaseSettings.cypher_planner -> GraphDatabaseSettings.CypherPlanner.COST)
      .preParseQuery("RETURN 1").options.queryOptions.planner shouldEqual CypherPlannerOption.cost

    preParserWith(GraphDatabaseInternalSettings.cypher_runtime -> GraphDatabaseInternalSettings.CypherRuntime.PIPELINED)
      .preParseQuery("RETURN 1").options.queryOptions.runtime shouldEqual CypherRuntimeOption.pipelined

    preParserWith(GraphDatabaseInternalSettings.cypher_expression_engine -> GraphDatabaseInternalSettings.CypherExpressionEngine.COMPILED)
      .preParseQuery("RETURN 1").options.queryOptions.expressionEngine shouldEqual CypherExpressionEngineOption.compiled

    preParserWith(GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.COMPILED)
      .preParseQuery("RETURN 1").options.queryOptions.operatorEngine shouldEqual CypherOperatorEngineOption.compiled

    preParserWith(GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.ALL)
      .preParseQuery("RETURN 1").options.queryOptions.interpretedPipesFallback shouldEqual CypherInterpretedPipesFallbackOption.allPossiblePlans
  }

  test("should not accept illegal combinations") {

    case class OptionCombo(
      optionA: Option,
      optionB: Option,
    )
    case class Option(
      asString: String,
      asSetting: (Setting[_], AnyRef),
    )

    val expressionEngineCompiled = Option("expressionEngine=compiled",
      GraphDatabaseInternalSettings.cypher_expression_engine -> GraphDatabaseInternalSettings.CypherExpressionEngine.COMPILED)
    val operatorEngineCompiled = Option("operatorEngine=compiled",
      GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.COMPILED)
    val interpretedPipesFallbackDisabled = Option("interpretedPipesFallback=disabled",
      GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.DISABLED)
    val interpretedPipesFallbackWhitelisted = Option("interpretedPipesFallback=whitelisted_plans_only",
      GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.WHITELISTED_PLANS_ONLY)
    val interpretedPipesFallbackAll = Option("interpretedPipesFallback=all",
      GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.ALL)
    val runtimeInterpreted = Option("runtime=interpreted",
      GraphDatabaseInternalSettings.cypher_runtime -> GraphDatabaseInternalSettings.CypherRuntime.INTERPRETED)
    val runtimeSlotted = Option("runtime=slotted",
      GraphDatabaseInternalSettings.cypher_runtime -> GraphDatabaseInternalSettings.CypherRuntime.SLOTTED)

    val invalidCombos = Seq(
      OptionCombo(expressionEngineCompiled, runtimeInterpreted),
      OptionCombo(operatorEngineCompiled, runtimeSlotted),
      OptionCombo(operatorEngineCompiled, runtimeInterpreted),
      OptionCombo(interpretedPipesFallbackDisabled, runtimeInterpreted),
      OptionCombo(interpretedPipesFallbackDisabled, runtimeSlotted),
      OptionCombo(interpretedPipesFallbackWhitelisted, runtimeInterpreted),
      OptionCombo(interpretedPipesFallbackWhitelisted, runtimeSlotted),
      OptionCombo(interpretedPipesFallbackAll, runtimeInterpreted),
      OptionCombo(interpretedPipesFallbackAll, runtimeSlotted),
    )

    def shouldFail(query: String, settings: (Setting[_], AnyRef)*) =
      withClue(s"query: $query, settings: $settings") {
        intercept[InvalidArgumentException](
          preParserWith(settings: _*).preParseQuery(query)
        )
      }

    invalidCombos.foreach { combo =>
      shouldFail(s"CYPHER ${combo.optionA.asString} ${combo.optionB.asString} RETURN 1")
      shouldFail(s"CYPHER ${combo.optionB.asString} RETURN 1", combo.optionA.asSetting)
      shouldFail(s"CYPHER ${combo.optionA.asString} RETURN 1", combo.optionB.asSetting)
      shouldFail(s"RETURN 1", combo.optionA.asSetting, combo.optionB.asSetting)
    }
  }
}
