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
package org.neo4j.cypher.internal.preparser

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.notification.DeprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.cypher.internal.notification.DeprecatedEagerAnalyzerPreParserOption
import org.neo4j.cypher.internal.notification.RecordingNotificationLogger
import org.neo4j.cypher.internal.notification.RetiredPlannerVersionPreParserOption
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.options.CypherRuntimeOption.slotted
import org.neo4j.cypher.internal.options.CypherVersionOption.cypher5
import org.neo4j.cypher.internal.options._
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.InvalidCypherOption
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.config.Setting

import scala.jdk.CollectionConverters.MapHasAsJava

class PreParserTest extends CommunityCypherTestSuite {

  private def preParserWith(settings: (Setting[_], AnyRef)*) = new CachingPreParser(
    CypherConfiguration.fromConfig(Config.defaults(settings.toMap.asJava)),
    new LFUCache[PreParsedQuery.CacheKey, PreParsedQuery](TestExecutorCaffeineCacheFactory, 0)
  )

  private val preParser = preParserWith()

  def preParse(queryText: String): PreParsedQuery =
    preParser.preParseQuery(queryText, devNullLogger, CypherVersion.Legacy.legacyVersion())

  test("should not allow inconsistent runtime options") {
    intercept[InvalidArgumentException](preParse("CYPHER runtime=slotted runtime=interpreted RETURN 42"))
  }

  test("should not allow both EXPLAIN and PROFILE") {
    intercept[InvalidArgumentException](preParse("EXPLAIN PROFILE RETURN 42"))
    intercept[InvalidArgumentException](preParse("PROFILE EXPLAIN RETURN 42"))
  }

  test("should not allow both SCOPE and PLAN") {
    intercept[InvalidArgumentException](preParse("EXPLAIN SCOPE EXPLAIN PLAN RETURN 42"))
    intercept[InvalidArgumentException](preParse("EXPLAIN PLAN EXPLAIN SCOPE RETURN 42"))
  }

  test("should throw error on empty query") {
    intercept[SyntaxException](preParse(""))
    intercept[SyntaxException](preParse("     "))
    intercept[SyntaxException](preParse("EXPLAIN\n\n\n\n\n"))
    intercept[SyntaxException](preParse("EXPLAIN   "))
    intercept[SyntaxException](preParse("EXPLAIN PLAN  "))
    intercept[SyntaxException](preParse("EXPLAIN SCOPE "))
    intercept[SyntaxException](preParse("PROFILE "))
    intercept[SyntaxException](preParse("CYPHER 25"))
  }

  val queries = Seq(
    "RETURN 1",
    "RETURN 1 RETURN",
    "RETURN 1 123RETURN",
    "#???#?#?#??//// 1 123RETURN",
    "RETURN 1 #RETURN",
    "RET#URN # #RETURN",
    "CYPHER RETURN 1",
    "CYPHER RETURN 1 RETURN",
    "CYPHER RETURN 1 123RETURN",
    "CYPHER #???#?#?#??//// 1 123RETURN",
    "CYPHER RETURN 1 #RETURN",
    "CYPHER RET#URN # #RETURN"
  )

  queries.foreach(q =>
    test(s"should preparse $q") {
      preParse(q).options.queryOptions shouldBe
        CypherQueryOptions.defaultOptions
    }
  )

  test("should preparse with unicode") {
    val res = preParse(
      "CYPHER 5\\u0020runtime=slotted \tRETURN\\u0020\\u0020\\u0020 AS res "
    )
    res.options.queryOptions shouldBe CypherQueryOptions.defaultOptions.copy(cypher5, runtime = slotted)
    res.statement shouldBe "RETURN\\u0020\\u0020\\u0020 AS res "
  }

  test("should preparse with unicode whitespace before query") {
    val res = preParse(
      "CYPHER 5\\u0020runtime=slotted \t\\u0020RETURN\\u0020\\u0020\\u0020 AS res "
    )
    res.options.queryOptions shouldBe CypherQueryOptions.defaultOptions.copy(cypher5, runtime = slotted)
    res.statement shouldBe "RETURN\\u0020\\u0020\\u0020 AS res "
  }

  test("should allow CYPHER version") {
    preParse("RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions
    preParse("CYPHER RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions

    preParse("CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(cypherVersion = CypherVersionOption.cypher5)
    preParse("CYPHER 5 CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(cypherVersion = CypherVersionOption.cypher5)
    preParse("CYPHER 5 runtime=slotted RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        runtime = CypherRuntimeOption.slotted
      )
    preParse("CYPHER 5 runtime=slotted replan=skip RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        runtime = CypherRuntimeOption.slotted,
        replan = CypherReplanOption.skip
      )
    preParse("EXPLAIN CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        executionMode = CypherExecutionMode.explain
      )
    preParse("EXPLAIN PLAN CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        executionMode = CypherExecutionMode.explain,
        planMode = CypherPlanMode.plan
      )
    preParserWith(
      GraphDatabaseInternalSettings.cypher_enable_scope_queries -> java.lang.Boolean.TRUE
    ).preParseQuery(
      "EXPLAIN SCOPE CYPHER 5 RETURN 42",
      devNullLogger,
      CypherVersion.Legacy.legacyVersion()
    ).options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        executionMode = CypherExecutionMode.explain,
        planMode = CypherPlanMode.scope
      )
    preParse("PROFILE CYPHER 5 RETURN 42").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        executionMode = CypherExecutionMode.profile
      )

    intercept[InvalidCypherOption](preParse("CYPHER 6 RETURN 42"))
      .getMessage shouldBe "6 is not a valid option for cypher version. Valid options are: 25, 5"
    intercept[InvalidCypherOption](preParse("CYPHER 26 RETURN 42"))
      .getMessage shouldBe "26 is not a valid option for cypher version. Valid options are: 25, 5"
    intercept[InvalidCypherOption](preParse("CYPHER 5.20 RETURN 42"))
      .getMessage shouldBe "5.20 is not a valid option for cypher version. Valid options are: 25, 5"
    intercept[InvalidCypherOption](preParse("CYPHER 4.4 RETURN 42"))
      .getMessage shouldBe "4.4 is not a valid option for cypher version. Valid options are: 25, 5"

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
      notificationLogger,
      CypherVersion.Legacy.legacyVersion()
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
      notificationLogger,
      CypherVersion.Legacy.legacyVersion()
    )
    notificationLogger.notifications should equal(Set(
      DeprecatedConnectComponentsPlannerPreParserOption(InputPosition(7, 1, 8))
    ))
    preParsedQuery.notifications should equal(Seq(
      DeprecatedConnectComponentsPlannerPreParserOption(InputPosition(7, 1, 8))
    ))
  }

  test("should issue a notification for deprecated eagerAnalyzer option") {
    val notificationLogger = new RecordingNotificationLogger()
    val preParsedQuery = preParser.preParseQuery(
      "CYPHER eagerAnalyzer=ir RETURN 42",
      notificationLogger,
      CypherVersion.Legacy.legacyVersion()
    )
    notificationLogger.notifications shouldEqual Set(
      DeprecatedEagerAnalyzerPreParserOption(InputPosition(7, 1, 8))
    )
    preParsedQuery.notifications shouldEqual List(
      DeprecatedEagerAnalyzerPreParserOption(InputPosition(7, 1, 8))
    )
  }

  test("should issue a notification for deprecated eagerAnalyzer option, even when setting it to LP eagerness") {
    val notificationLogger = new RecordingNotificationLogger()
    val preParsedQuery = preParser.preParseQuery(
      "CYPHER eagerAnalyzer=lp RETURN 42",
      notificationLogger,
      CypherVersion.Legacy.legacyVersion()
    )
    notificationLogger.notifications shouldEqual Set(
      DeprecatedEagerAnalyzerPreParserOption(InputPosition(7, 1, 8))
    )
    preParsedQuery.notifications shouldEqual List(
      DeprecatedEagerAnalyzerPreParserOption(InputPosition(7, 1, 8))
    )
  }

  test("should issue a notification for retired planner version v2026_03 and remap to default") {
    val notificationLogger = new RecordingNotificationLogger()
    val preParsedQuery = preParser.preParseQuery(
      "CYPHER plannerVersion=v2026_03 RETURN 42",
      notificationLogger,
      CypherVersion.Legacy.legacyVersion()
    )
    notificationLogger.notifications shouldEqual Set(
      RetiredPlannerVersionPreParserOption(InputPosition(7, 1, 8), "v2026_03")
    )
    preParsedQuery.notifications shouldEqual List(
      RetiredPlannerVersionPreParserOption(InputPosition(7, 1, 8), "v2026_03")
    )
    preParsedQuery.options.queryOptions.plannerVersionOption shouldBe CypherPlannerVersionOption.default
  }

  test("should issue a notification for retired planner version with case-insensitive option key") {
    val notificationLogger = new RecordingNotificationLogger()
    val preParsedQuery = preParser.preParseQuery(
      "CYPHER PLANNERVERSION=v2026_03 RETURN 42",
      notificationLogger,
      CypherVersion.Legacy.legacyVersion()
    )
    notificationLogger.notifications shouldEqual Set(
      RetiredPlannerVersionPreParserOption(InputPosition(7, 1, 8), "v2026_03")
    )
    preParsedQuery.notifications shouldEqual List(
      RetiredPlannerVersionPreParserOption(InputPosition(7, 1, 8), "v2026_03")
    )
    preParsedQuery.options.queryOptions.plannerVersionOption shouldBe CypherPlannerVersionOption.latest
  }

  test("should issue a notification for retired planner version with case-insensitive option value") {
    val notificationLogger = new RecordingNotificationLogger()
    val preParsedQuery = preParser.preParseQuery(
      "CYPHER plannerVersion=V2026_03 RETURN 42",
      notificationLogger,
      CypherVersion.Legacy.legacyVersion()
    )
    notificationLogger.notifications shouldEqual Set(
      RetiredPlannerVersionPreParserOption(InputPosition(7, 1, 8), "V2026_03")
    )
    preParsedQuery.notifications shouldEqual List(
      RetiredPlannerVersionPreParserOption(InputPosition(7, 1, 8), "V2026_03")
    )
    preParsedQuery.options.queryOptions.plannerVersionOption shouldBe CypherPlannerVersionOption.latest
  }

  test("should not issue a retired planner version notification for non-retired versions") {
    val nonRetiredVersions = Seq("v2026_04", "experimental", "next", "latest")
    nonRetiredVersions.foreach { version =>
      withClue(s"plannerVersion=$version") {
        val notificationLogger = new RecordingNotificationLogger()
        val preParsedQuery = preParser.preParseQuery(
          s"CYPHER plannerVersion=$version RETURN 42",
          notificationLogger,
          CypherVersion.Legacy.legacyVersion()
        )
        notificationLogger.notifications.collect {
          case n: RetiredPlannerVersionPreParserOption => n
        } shouldBe empty
        preParsedQuery.notifications.collect {
          case n: RetiredPlannerVersionPreParserOption => n
        } shouldBe empty
      }
    }
  }

  test("should not allow multiple conflicting replan strategies") {
    intercept[InvalidArgumentException](preParse("CYPHER replan=force replan=skip RETURN 42"))
  }

  test("should accept parallelRepeatHeuristic option") {
    preParse(
      "CYPHER parallelRepeatHeuristic=enabled RETURN 42"
    ).options.queryOptions.parallelRepeatHeuristic should equal(CypherParallelRepeatHeuristicOption.enabled)
    preParse(
      "CYPHER parallelRepeatHeuristic=disabled RETURN 42"
    ).options.queryOptions.parallelRepeatHeuristic should equal(CypherParallelRepeatHeuristicOption.disabled)
    preParse(
      "RETURN 42"
    ).options.queryOptions.parallelRepeatHeuristic should equal(CypherParallelRepeatHeuristicOption.disabled)
  }

  test("should not allow multiple conflicting parallelRepeatHeuristic options") {
    intercept[InvalidArgumentException](
      preParse("CYPHER parallelRepeatHeuristic=enabled parallelRepeatHeuristic=disabled RETURN 42")
    )
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
    preParserWith(
      GraphDatabaseSettings.cypher_planner -> GraphDatabaseSettings.CypherPlanner.COST
    )
      .preParseQuery(
        "RETURN 1",
        devNullLogger,
        CypherVersion.Legacy.legacyVersion()
      ).options.queryOptions.planner shouldEqual
      CypherPlannerOption.cost

    preParserWith(GraphDatabaseInternalSettings.cypher_runtime -> GraphDatabaseInternalSettings.CypherRuntime.PIPELINED)
      .preParseQuery(
        "RETURN 1",
        devNullLogger,
        CypherVersion.Legacy.legacyVersion()
      ).options.queryOptions.runtime shouldEqual
      CypherRuntimeOption.pipelined

    preParserWith(
      GraphDatabaseInternalSettings.cypher_expression_engine -> GraphDatabaseInternalSettings.CypherExpressionEngine.COMPILED
    )
      .preParseQuery(
        "RETURN 1",
        devNullLogger,
        CypherVersion.Legacy.legacyVersion()
      ).options.queryOptions.expressionEngine shouldEqual
      CypherExpressionEngineOption.compiled

    preParserWith(
      GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.COMPILED
    )
      .preParseQuery(
        "RETURN 1",
        devNullLogger,
        CypherVersion.Legacy.legacyVersion()
      ).options.queryOptions.operatorEngine shouldEqual
      CypherOperatorEngineOption.compiled

    preParserWith(
      GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback.ALL
    )
      .preParseQuery("RETURN 1", devNullLogger, CypherVersion.Legacy.legacyVersion())
      .options.queryOptions.interpretedPipesFallback shouldEqual CypherInterpretedPipesFallbackOption.allPossiblePlans
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
          preParserWith(settings: _*).preParseQuery(query, devNullLogger, CypherVersion.Legacy.legacyVersion())
        )
      }

    invalidCombos.foreach { combo =>
      shouldFail(s"CYPHER ${combo.optionA.asString} ${combo.optionB.asString} RETURN 1")
      shouldFail(s"CYPHER ${combo.optionB.asString} RETURN 1", combo.optionA.asSetting)
      shouldFail(s"CYPHER ${combo.optionA.asString} RETURN 1", combo.optionB.asSetting)
      shouldFail(s"RETURN 1", combo.optionA.asSetting, combo.optionB.asSetting)
    }
  }

  test("preparser should throw error on invalid query option") {
    val query = "cypher röntime=chevacheville65 return 'wroom'"

    the[InvalidCypherOption] thrownBy {
      preParse(query)
    } should have message "Unsupported options: röntime"
  }

  test("preparser should throw error on invalid query option with comments") {
    val query = """
                  |// hello
                  |cypher /*Hello*/ ranÄäöäötime=/*comment*/chevac/**/hev/*hrrhrh*/ille65 return 'wroom'""".stripMargin

    the[InvalidCypherOption] thrownBy {
      preParse(query)
    } should have message "Unsupported options: ranääöäötime"
  }

  test("preparser should throw error on invalid query option value with comments") {
    val query = """
                  |// hello
                  |cypher /*Hello*/ runtime=/*comment*/chevac/**/hev/*hrrhrh*/ille65 return 'wroom'""".stripMargin

    the[InvalidCypherOption] thrownBy {
      preParse(query)
    } should have message "chevac is not a valid option for runtime. Valid options are: interpreted, legacy, parallel, pipelined, slotted"
  }

  // Will fail in query parsing
  test("version as key") {
    preParse("cypher 5=slotted return 1").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(cypherVersion = CypherVersionOption.cypher5)
  }

  test("invalid key in option") {
    val query = "cypher röntime=slotted return 1"

    the[InvalidCypherOption] thrownBy {
      preParse(query)
    } should have message "Unsupported options: röntime"
  }

  test("only unicode") {
    // cypher 5 runtime=slotted return 1
    val query =
      "\u0063\u0079\u0070\u0068\u0065\u0072\u0020\u0035\u0020\u0072\u0075\u006E\u0074\u0069\u006D\u0065\u003D\u0073\u006C\u006F\u0074\u0074\u0065\u0064\u0020\u0072\u0065\u0074\u0075\u0072\u006E\u0020\u0031"
    preParse(query).options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        runtime = CypherRuntimeOption.slotted
      )
  }

  test("only unicode error") {
    // cypher 5 räntime=slotted return 1
    val query =
      "\u0063\u0079\u0070\u0068\u0065\u0072\u0020\u0035\u0020\u0072\u00C4\u006E\u0074\u0069\u006D\u0065\u003D\u0073\u006C\u006F\u0074\u0074\u0065\u0064\u0020\u0072\u0065\u0074\u0075\u0072\u006E\u0020\u0031"

    the[InvalidCypherOption] thrownBy {
      preParse(query)
    } should have message "Unsupported options: räntime"
  }

  test("option with comment and linebreak") {
    preParse("cypher 5 runtime=//hej\nslotted return 1").options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        runtime = CypherRuntimeOption.slotted
      )
  }

  test("preparser option with comments") {
    preParse(
      "/*hej*/cypher/*hej*/5/*hej*/runtime/*hej*/=/*hej*/slotted/*hej*/return/*hej*/1"
    ).options.queryOptions shouldBe
      CypherQueryOptions.defaultOptions.copy(
        cypherVersion = CypherVersionOption.cypher5,
        runtime = CypherRuntimeOption.slotted
      )
  }

  test("invalid Cypher option") {
    val query = "CYPHER x=1 RETURN 1 AS x"

    the[InvalidCypherOption] thrownBy {
      preParse(query)
    } should have message "Unsupported options: x"
  }
}
