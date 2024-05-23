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
package org.neo4j.cypher.internal

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.util.DeprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.InvalidCypherOption
import org.neo4j.graphdb.config.Setting

import scala.jdk.CollectionConverters.MapHasAsJava

class PreParserTest extends CypherFunSuite {

  private def preParserWith(settings: (Setting[_], AnyRef)*) = new CachingPreParser(
    CypherConfiguration.fromConfig(Config.defaults(settings.toMap.asJava)),
    new LFUCache[String, PreParsedQuery](TestExecutorCaffeineCacheFactory, 0)
  )

  private val preParser = preParserWith()

  def preParse(queryText: String): PreParsedQuery = preParser.preParseQuery(queryText, devNullLogger)

  test("should not allow inconsistent runtime options") {
    intercept[InvalidArgumentException](preParse("CYPHER runtime=slotted runtime=interpreted RETURN 42"))
  }

  test("should not allow both EXPLAIN and PROFILE") {
    intercept[InvalidArgumentException](preParse("EXPLAIN PROFILE RETURN 42"))
    intercept[InvalidArgumentException](preParse("PROFILE EXPLAIN RETURN 42"))
  }

  test("should allow CYPHER version") {
    preParse("RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions
    preParse("CYPHER RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions

    preParse("CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(cypherVersion = CypherVersion.cypher5)
    preParse("CYPHER 5 CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(cypherVersion = CypherVersion.cypher5)
    preParse("CYPHER 5 runtime=slotted RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersion.cypher5,
        runtime = CypherRuntimeOption.slotted
      )
    preParse("CYPHER 5 runtime=slotted replan=skip RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersion.cypher5,
        runtime = CypherRuntimeOption.slotted,
        replan = CypherReplanOption.skip
      )
    preParse("EXPLAIN CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersion.cypher5,
        executionMode = CypherExecutionMode.explain
      )
    preParse("PROFILE CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersion.cypher5,
        executionMode = CypherExecutionMode.profile
      )

    intercept[InvalidCypherOption](preParse("CYPHER 6 RETURN 42"))
      .getMessage shouldBe "6 is not a valid option for cypher version. Valid options are: 5"
    intercept[InvalidCypherOption](preParse("CYPHER 5.20 RETURN 42"))
      .getMessage shouldBe "5.20 is not a valid option for cypher version. Valid options are: 5"
    intercept[InvalidCypherOption](preParse("CYPHER 4.4 RETURN 42"))
      .getMessage shouldBe "4.4 is not a valid option for cypher version. Valid options are: 5"

    // Not optimal, but not a lot we can do
    preParse("CYPHER gql RETURN 42").options.queryOptions shouldBe CypherQueryOptions.defaultOptions
  }

  test("should not allow unknown options") {
    intercept[InvalidArgumentException](preParse("CYPHER foo=idp RETURN 42"))
      .getMessage should include("foo")
  }

  test("should not allow unknown debug flags") {
    intercept[InvalidArgumentException](preParse("CYPHER debug=idp RETURN 42"))
      .getMessage should include("idp")
  }

  test("should accept just one operator execution mode") {
    preParse(
      "CYPHER operatorEngine=interpreted RETURN 42"
    ).options.queryOptions.operatorEngine should equal(CypherOperatorEngineOption.interpreted)
  }

  test("cacheKey with EXPLAIN") {
    val q1 = preParse("CYPHER runtime=slotted operatorEngine=interpreted RETURN 42")
    val q2 = preParse("EXPLAIN CYPHER runtime=slotted operatorEngine=interpreted RETURN 42")
    val q3 = preParse("CYPHER runtime=slotted operatorEngine=interpreted EXPLAIN RETURN 42")
    val q4 = preParse("CYPHER runtime=slotted EXPLAIN operatorEngine=interpreted RETURN 42")

    q1.cacheKey.should(equal(q2.cacheKey))
    q2.cacheKey.should(equal(q3.cacheKey))
    q3.cacheKey.should(not(equal(q4.cacheKey)))
  }

  test("cacheKey with PROFILE") {
    val q1 = preParse("CYPHER runtime=slotted operatorEngine=interpreted RETURN 42")
    val q2 = preParse("PROFILE CYPHER runtime=slotted operatorEngine=interpreted RETURN 42")
    val q3 = preParse("CYPHER runtime=slotted operatorEngine=interpreted PROFILE RETURN 42")
    val q4 = preParse("CYPHER runtime=slotted PROFILE operatorEngine=interpreted RETURN 42")

    q1.cacheKey.should(not(equal(q2.cacheKey)))
    q2.cacheKey.should(equal(q3.cacheKey))
    q3.cacheKey.should(not(equal(q4.cacheKey)))
  }

  test("cacheKey for mixed queries") {
    val queries = Seq(
      "CYPHER runtime=slotted PROFILE RETURN 1",
      "CYPHER PROFILE runtime=slotted RETURN 1",
      "PROFILE CYPHER runtime=slotted RETURN 1",
      // EXPLAIN
      "CYPHER runtime=slotted EXPLAIN RETURN 1",
      "CYPHER EXPLAIN runtime=slotted RETURN 1",
      "EXPLAIN CYPHER runtime=slotted RETURN 1",
      "CYPHER runtime=slotted RETURN 1",
      // PROFILE with multiple options
      "CYPHER runtime=slotted planner=dp PROFILE RETURN 1",
      "CYPHER PROFILE planner=dp runtime=slotted RETURN 1",
      "PROFILE CYPHER planner=dp runtime=slotted RETURN 1",
      // EXPLAIN with multiple options
      "CYPHER runtime=slotted planner=dp EXPLAIN RETURN 1",
      "CYPHER EXPLAIN planner=dp runtime=slotted RETURN 1",
      "EXPLAIN CYPHER planner=dp runtime=slotted RETURN 1",
      // plain with multiple options
      "CYPHER planner=dp runtime=slotted RETURN 1",
      "CYPHER planner=dp runtime=slotted debug=toString RETURN 1",
      "CYPHER planner=dp runtime=slotted debug=toString RETURN 1"
    )

    val cacheKeys = queries.map(preParse).map(_.cacheKey).toSet
    cacheKeys.size.shouldEqual(9)
  }

  test("should accept just one replan strategy") {
    preParse("CYPHER replan=force RETURN 42").options.queryOptions.replan should equal(
      CypherReplanOption.force
    )
    preParse("CYPHER replan=skip RETURN 42").options.queryOptions.replan should equal(
      CypherReplanOption.skip
    )
    preParse("CYPHER replan=default RETURN 42").options.queryOptions.replan should equal(
      CypherReplanOption.default
    )
  }

  test("should accept just one connect components planner") {
    preParse(
      "CYPHER connectComponentsPlanner=idp RETURN 42"
    ).options.queryOptions.connectComponentsPlanner should equal(CypherConnectComponentsPlannerOption.idp)
  }

  test("should issue a notification for connectComponentsPlanner") {
    val notificationLogger = new RecordingNotificationLogger()
    val preParsedQuery = preParser.preParseQuery(
      "CYPHER connectComponentsPlanner=idp RETURN 42",
      notificationLogger
    )
    notificationLogger.notifications should equal(Set(
      DeprecatedConnectComponentsPlannerPreParserOption(InputPosition(7, 1, 8))
    ))
    preParsedQuery.notifications should equal(Seq(
      DeprecatedConnectComponentsPlannerPreParserOption(InputPosition(7, 1, 8))
    ))
  }

  test("should issue a notification for cOnnectcomPONonentsPlanner") {
    val notificationLogger = new RecordingNotificationLogger()
    val preParsedQuery = preParser.preParseQuery(
      "CYPHER cOnnectcomPONentsPlanner=idp RETURN 42",
      notificationLogger
    )
    notificationLogger.notifications should equal(Set(
      DeprecatedConnectComponentsPlannerPreParserOption(InputPosition(7, 1, 8))
    ))
    preParsedQuery.notifications should equal(Seq(
      DeprecatedConnectComponentsPlannerPreParserOption(InputPosition(7, 1, 8))
    ))
  }

  test("should not allow multiple conflicting replan strategies") {
    intercept[InvalidArgumentException](preParse("CYPHER replan=force replan=skip RETURN 42"))
  }

  test("should accept just one interpreted pipes fallback mode") {
    preParse(
      "CYPHER interpretedPipesFallback=disabled RETURN 42"
    ).options.queryOptions.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.disabled)
    preParse(
      "CYPHER interpretedPipesFallback=default RETURN 42"
    ).options.queryOptions.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.default)
    preParse(
      "CYPHER interpretedPipesFallback=whitelisted_plans_only RETURN 42"
    ).options.queryOptions.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.whitelistedPlansOnly)
    preParse(
      "CYPHER interpretedPipesFallback=all RETURN 42"
    ).options.queryOptions.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.allPossiblePlans)
  }

  test("should not allow multiple conflicting interpreted pipes fallback modes") {
    intercept[InvalidArgumentException](
      preParse("CYPHER interpretedPipesFallback=all interpretedPipesFallback=disabled RETURN 42")
    )
    intercept[InvalidArgumentException](
      preParse("CYPHER interpretedPipesFallback=default interpretedPipesFallback=disabled RETURN 42")
    )
    intercept[InvalidArgumentException](
      preParse("CYPHER interpretedPipesFallback=default interpretedPipesFallback=all RETURN 42")
    )
  }

  test("should only allow interpreted pipes fallback mode in pipelined runtime") {
    intercept[InvalidArgumentException](
      preParse("CYPHER runtime=slotted interpretedPipesFallback=all RETURN 42")
    )
  }

  test("should not allow multiple conflicting connect component planners") {
    intercept[InvalidArgumentException](
      preParse("CYPHER connectComponentsPlanner=idp connectComponentsPlanner=greedy RETURN 42")
    )
    intercept[InvalidArgumentException](
      preParse("CYPHER connectComponentsPlanner=greedy connectComponentsPlanner=idp RETURN 42")
    )
  }

  test("should take defaults from config") {
    preParserWith(GraphDatabaseSettings.cypher_planner -> GraphDatabaseSettings.CypherPlanner.COST)
      .preParseQuery("RETURN 1", devNullLogger).options.queryOptions.planner shouldEqual CypherPlannerOption.cost

    preParserWith(GraphDatabaseInternalSettings.cypher_runtime -> GraphDatabaseInternalSettings.CypherRuntime.PIPELINED)
      .preParseQuery("RETURN 1", devNullLogger).options.queryOptions.runtime shouldEqual CypherRuntimeOption.pipelined

    preParserWith(
      GraphDatabaseInternalSettings.cypher_expression_engine -> GraphDatabaseInternalSettings.CypherExpressionEngine.COMPILED
    )
      .preParseQuery(
        "RETURN 1",
        devNullLogger
      ).options.queryOptions.expressionEngine shouldEqual CypherExpressionEngineOption.compiled

    preParserWith(
      GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.COMPILED
    )
      .preParseQuery(
        "RETURN 1",
        devNullLogger
      ).options.queryOptions.operatorEngine shouldEqual CypherOperatorEngineOption.compiled

    preParserWith(
      GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.ALL
    )
      .preParseQuery(
        "RETURN 1",
        devNullLogger
      ).options.queryOptions.interpretedPipesFallback shouldEqual CypherInterpretedPipesFallbackOption.allPossiblePlans
  }

  test("should not accept illegal combinations") {

    case class OptionCombo(
      optionA: Option,
      optionB: Option
    )
    case class Option(
      asString: String,
      asSetting: (Setting[_], AnyRef)
    )

    val expressionEngineCompiled = Option(
      "expressionEngine=compiled",
      GraphDatabaseInternalSettings.cypher_expression_engine -> GraphDatabaseInternalSettings.CypherExpressionEngine.COMPILED
    )
    val operatorEngineCompiled = Option(
      "operatorEngine=compiled",
      GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.COMPILED
    )
    val interpretedPipesFallbackDisabled = Option(
      "interpretedPipesFallback=disabled",
      GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.DISABLED
    )
    val interpretedPipesFallbackWhitelisted = Option(
      "interpretedPipesFallback=whitelisted_plans_only",
      GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.WHITELISTED_PLANS_ONLY
    )
    val interpretedPipesFallbackAll = Option(
      "interpretedPipesFallback=all",
      GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.ALL
    )
    val runtimeInterpreted = Option(
      "runtime=legacy",
      GraphDatabaseInternalSettings.cypher_runtime -> GraphDatabaseInternalSettings.CypherRuntime.LEGACY
    )
    val runtimeSlotted = Option(
      "runtime=slotted",
      GraphDatabaseInternalSettings.cypher_runtime -> GraphDatabaseInternalSettings.CypherRuntime.SLOTTED
    )

    val invalidCombos = Seq(
      OptionCombo(expressionEngineCompiled, runtimeInterpreted),
      OptionCombo(operatorEngineCompiled, runtimeSlotted),
      OptionCombo(operatorEngineCompiled, runtimeInterpreted),
      OptionCombo(interpretedPipesFallbackDisabled, runtimeInterpreted),
      OptionCombo(interpretedPipesFallbackDisabled, runtimeSlotted),
      OptionCombo(interpretedPipesFallbackWhitelisted, runtimeInterpreted),
      OptionCombo(interpretedPipesFallbackWhitelisted, runtimeSlotted),
      OptionCombo(interpretedPipesFallbackAll, runtimeInterpreted),
      OptionCombo(interpretedPipesFallbackAll, runtimeSlotted)
    )

    def shouldFail(query: String, settings: (Setting[_], AnyRef)*) =
      withClue(s"query: $query, settings: $settings") {
        intercept[InvalidArgumentException](
          preParserWith(settings: _*).preParseQuery(query, devNullLogger)
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
