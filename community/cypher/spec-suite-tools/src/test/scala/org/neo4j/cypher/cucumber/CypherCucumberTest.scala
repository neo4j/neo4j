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
package org.neo4j.cypher.cucumber

import io.cucumber.core.backend.ObjectFactory
import io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME
import io.cucumber.junit.platform.engine.Constants.JUNIT_PLATFORM_LONG_NAMING_STRATEGY_EXAMPLE_NAME_PROPERTY_NAME
import io.cucumber.junit.platform.engine.Constants.JUNIT_PLATFORM_NAMING_STRATEGY_PROPERTY_NAME
import io.cucumber.junit.platform.engine.Constants.OBJECT_FACTORY_PROPERTY_NAME
import org.assertj.core.api.Assertions.assertThat
import org.junit.platform.engine.TestExecutionResult
import org.junit.platform.engine.discovery.DiscoverySelectors
import org.junit.platform.launcher.EngineFilter
import org.junit.platform.launcher.TestExecutionListener
import org.junit.platform.launcher.TestIdentifier
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder
import org.junit.platform.launcher.core.LauncherFactory
import org.junit.platform.launcher.listeners.SummaryGeneratingListener
import org.neo4j.cypher.cucumber.CypherCucumberTest.TestConfiguration
import org.neo4j.cypher.cucumber.glue.obfuscator.ObfuscatorSteps
import org.neo4j.cypher.cucumber.glue.regular.InjectedTestConf
import org.neo4j.cypher.cucumber.glue.regular.SingletonInjector
import org.neo4j.cypher.cucumber.glue.regular.TestConf
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps
import org.neo4j.cypher.cucumber.synthesise.read.ScenarioReader
import org.neo4j.cypher.internal.compiler.planner.CypherPlannerVersionWithOptimisations
import org.neo4j.cypher.internal.options.CypherPlannerVersionOption
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.LoneElement

import java.io.ByteArrayOutputStream
import java.io.PrintWriter
import java.lang.reflect.Modifier
import java.util
import java.util.Collections
import java.util.ServiceLoader
import java.util.function.Consumer

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

class CypherCucumberTest extends CypherFunSuite with LoneElement {

  test("cucumber based cypher tests can fail and pass in various ways", Tags.NoSpdOverride) {
    val request = LauncherDiscoveryRequestBuilder.request()
      .filters(EngineFilter.includeEngines("cucumber"))
      .selectors(DiscoverySelectors.selectPackage("test.features"))
      .configurationParameter(GLUE_PROPERTY_NAME, CypherCucumber.Glue.IgnoreFailTaggedScenarios)
      .configurationParameter(JUNIT_PLATFORM_NAMING_STRATEGY_PROPERTY_NAME, "long")
      .configurationParameter(JUNIT_PLATFORM_LONG_NAMING_STRATEGY_EXAMPLE_NAME_PROPERTY_NAME, "number")
      .configurationParameter(OBJECT_FACTORY_PROPERTY_NAME, classOf[TestConfiguration.ObjectFactory].getName)
      .build()

    val summaryListener = new SummaryGeneratingListener()
    val passingListener = new CypherCucumberTest.TestListener
    LauncherFactory.create().execute(request, summaryListener, passingListener)

    val summary = summaryListener.getSummary

    val summaryOutputStream = new ByteArrayOutputStream()
    val summaryString = new PrintWriter(summaryOutputStream)
    summary.printTo(summaryString)
    summaryString.println("\n\n===== FAILURES =====\n")
    summary.printFailuresTo(summaryString)
    summaryString.flush()

    // Test counts should be correct
    withClue(summaryOutputStream.toString) {
      summary.getTestsSucceededCount shouldBe 30
      summary.getContainersFailedCount shouldBe 0
      summary.getTestsFoundCount shouldBe 164
      summary.getTestsFailedCount shouldBe 134
      summary.getTestsAbortedCount shouldBe 0
      summary.getTestsSkippedCount shouldBe 0
    }

    // Passing tests should pass
    assertThat(passingListener.passing.toArray(new Array[String](0)).sorted)
      .describedAs("Tests that are expected to succeed actually succeeds")
      .containsExactly(
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.1",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.10",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.11",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.12",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.13",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.14",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.15",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.16",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.17",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.18",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.19",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.2",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.20",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.21",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.3",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.4",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.5",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.6",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.7",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.8",
        "TestFrameworkTests - [016] Most types work in cucumber tests - Examples - Example #1.9",
        "TestFrameworkTests - [040] Warning has correct code",
        "TestFrameworkTests - [041] Warning has correct code and correct message",
        "TestFrameworkTests - [042] Floating point precision can be specified - Examples - Example #1.1",
        "TestFrameworkTests - [042] Floating point precision can be specified - Examples - Example #1.2",
        "TestFrameworkTests - [042] Floating point precision can be specified - Examples - Example #1.3",
        "TestFrameworkTests - [042] Floating point precision can be specified - Examples - Example #1.4",
        "TestFrameworkTests - [044] Syntax error is correct",
        "TestFrameworkTests - [053] Approximate result - exact match",
        "TestFrameworkTests - [054] Approximate result - without optional row"
      )

    // Failing tests should fail in the correct way
    val sortedFailures = summary.getFailures.asScala.view
      .map(f => CypherCucumberTest.Failure(f.getTestIdentifier.getDisplayName, f.getException))
      .sortBy(_.testName)
      .toArray
    assertThat(sortedFailures)
      .describedAs("Failing tests should fail in the correct way")
      .satisfiesExactly(
        wrongResultOrdered("[001] Incorrect result value ordered"),
        wrongResultAnyOrder("[002] Incorrect result value any order"),
        wrongResultOrderedAnyListOrder(
          "[003] Incorrect result value ordered, any list order - Examples - Example #1.1"
        ),
        wrongResultOrderedAnyListOrder(
          "[003] Incorrect result value ordered, any list order - Examples - Example #1.2"
        ),
        wrongResultOrderedAnyListOrder(
          "[003] Incorrect result value ordered, any list order - Examples - Example #1.3"
        ),
        wrongResultAnyOrderAnyListOrder(
          "[004] Incorrect result value any order, any list order - Examples - Example #1.1"
        ),
        wrongResultAnyOrderAnyListOrder(
          "[004] Incorrect result value any order, any list order - Examples - Example #1.2"
        ),
        wrongResultAnyOrderAnyListOrder(
          "[004] Incorrect result value any order, any list order - Examples - Example #1.3"
        ),
        wrongResultOrdered("[005] Incorrect row count ordered"),
        wrongResultAnyOrder("[006] Incorrect row count any order"),
        wrongResultOrderedAnyListOrder("[007] Incorrect row count ordered, any list order"),
        wrongResultAnyOrderAnyListOrder("[008] Incorrect row count any order, any list order"),
        wrongHeaders("[009] Incorrect result headers ordered"),
        wrongHeaders("[010] Incorrect result headers any order"),
        wrongHeaders("[011] Incorrect result headers ordered, any list order"),
        wrongHeaders("[012] Incorrect result headers any order, any list order"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.1"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.10"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.11"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.12"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.13"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.14"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.2"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.3"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.4"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.5"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.6"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.7"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.8"),
        wrongSideEffects("[013] Incorrect side effects - Examples - Example #1.9"),
        queryFailedCompile("[014] Query failure - Examples - Example #1.1"),
        queryFailedRuntime("[014] Query failure - Examples - Example #1.2"),
        wrongConf("[015] Incorrect config"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.1"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.10"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.11"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.12"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.13"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.14"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.15"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.16"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.17"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.18"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.19"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.2"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.20"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.21"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.22"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.23"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.3"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.4"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.5"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.6"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.7"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.8"),
        wrongResultOrdered("[017] Most storable types can fail in cucumber tests - Examples - Example #1.9"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.1"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.10"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.11"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.12"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.13"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.14"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.15"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.16"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.17"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.18"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.19"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.2"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.20"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.21"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.22"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.23"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.3"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.4"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.5"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.6"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.7"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.8"),
        wrongResultOrdered("[018] Most types can fail in cucumber tests - Examples - Example #1.9"),
        wrongConfTag("[019] Incorrect conf tag"),
        wrongResultOrdered("[023] Open tx: Incorrect result value ordered"),
        wrongResultAnyOrder("[024] Open tx: Incorrect result value any order"),
        wrongResultOrderedAnyListOrder(
          "[025] Open tx: Incorrect result value ordered, any list order - Examples - Example #1.1"
        ),
        wrongResultOrderedAnyListOrder(
          "[025] Open tx: Incorrect result value ordered, any list order - Examples - Example #1.2"
        ),
        wrongResultOrderedAnyListOrder(
          "[025] Open tx: Incorrect result value ordered, any list order - Examples - Example #1.3"
        ),
        wrongResultAnyOrderAnyListOrder(
          "[026] Open tx: Incorrect result value any order, any list order - Examples - Example #1.1"
        ),
        wrongResultAnyOrderAnyListOrder(
          "[026] Open tx: Incorrect result value any order, any list order - Examples - Example #1.2"
        ),
        wrongResultAnyOrderAnyListOrder(
          "[026] Open tx: Incorrect result value any order, any list order - Examples - Example #1.3"
        ),
        wrongResultOrdered("[027] Open tx: Incorrect row count ordered"),
        wrongResultAnyOrder("[028] Open tx: Incorrect row count any order"),
        wrongResultOrderedAnyListOrder("[029] Open tx: Incorrect row count ordered, any list order"),
        wrongResultAnyOrderAnyListOrder("[030] Open tx: Incorrect row count any order, any list order"),
        wrongHeaders("[031] Open tx: Incorrect result headers ordered"),
        wrongHeaders("[032] Open tx: Incorrect result headers any order"),
        wrongHeaders("[033] Open tx: Incorrect result headers ordered, any list order"),
        wrongHeaders("[034] Open tx: Incorrect result headers any order, any list order"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.1"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.10"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.11"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.12"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.13"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.14"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.2"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.3"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.4"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.5"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.6"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.7"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.8"),
        wrongSideEffects("[035] Open tx: Incorrect side effects - Examples - Example #1.9"),
        queryFailedCompile("[036] Open tx: Query failure - Examples - Example #1.1"),
        queryFailedRuntime("[036] Open tx: Query failure - Examples - Example #1.2"),
        wrongWarningGqlCode("[037] Warning has incorrect code"),
        wrongWarningGqlCode("[038] Warning has incorrect code and correct message"),
        wrongWarningMessage("[039] Warning has correct code and incorrect message"),
        wrongResultOrdered("[043] Floating point precision is exact by default - Examples - Example #1.1"),
        wrongResultOrdered("[043] Floating point precision is exact by default - Examples - Example #1.2"),
        wrongResultOrdered("[043] Floating point precision is exact by default - Examples - Example #1.3"),
        wrongFailureMessage("[045] Syntax error has incorrect code 1"),
        wrongFailureMessage("[046] Syntax error has incorrect code 2"),
        wrongFailureMessage("[047] Syntax error has incorrect classification 1"),
        wrongFailureMessage("[048] Syntax error has incorrect classification 2"),
        wrongFailureMessage("[049] Syntax error has incorrect message 1"),
        wrongFailureMessage("[050] Syntax error has incorrect message 3"),
        wrongFailureMessage("[051] Syntax error has incorrect message 4"),
        wrongFailureMessage("[052] Syntax error has incorrect error count"),
        wrongResultOrdered("[055] Approximate result - without mandatory row"),
        wrongResultOrdered("[056] Approximate result - with extra row"),
        wrongResultOrdered("[057] Approximate result - with wrong order"),
        wrongNbrResults("[058] Approximate result - with wrong number of results"),
        wrongApproximateMandatoryColumn("[059] Approximate result - without mandatory column"),
        wrongApproximateMandatoryColumn("[060] Approximate result - with wrongly named mandatory column"),
        wrongApproximateMandatoryType("[061] Approximate result - with wrong type in mandatory column"),
        wrongWarningGqlCode("[062] Query has warnings but asserts on no warnings")
      )
  }

  test("scenario reader works") {
    val allScenarios = new ScenarioReader().readAllScenarios("test.features")
    allScenarios.size shouldBe 164
    // TODO assert read/write round trip.
  }

  test("object factories have correct names", Tags.NoSpdOverride) {
    val testConfs = ServiceLoader.load(classOf[ObjectFactory]).stream().toList.asScala.toSeq
      .map(_.get())
      .collect { case factory: SingletonInjector if Try(factory.getInstance(classOf[TestConf])).isSuccess => factory }
      .map(f => f.getClass.getName -> f.getInstance(classOf[TestConf]))
      .toMap

    val expected = Map(
      classOf[CypherCucumberTest.TestConfiguration.ObjectFactory].getName -> CypherCucumberTest.TestConfiguration.conf,
      TestConf.Default.Cypher25.FactoryName -> TestConf.Default.Cypher25.conf,
      TestConf.Default.Cypher5.FactoryName -> TestConf.Default.Cypher5.conf,
      TestConf.DefaultBolt.Cypher25.FactoryName -> TestConf.DefaultBolt.Cypher25.conf,
      TestConf.DefaultBolt.Cypher5.FactoryName -> TestConf.DefaultBolt.Cypher5.conf,
      TestConf.Pipelined.Cypher25.FactoryName -> TestConf.Pipelined.Cypher25.conf,
      TestConf.Pipelined.Cypher5.FactoryName -> TestConf.Pipelined.Cypher5.conf,
      TestConf.PipelinedFallback.Cypher25.FactoryName -> TestConf.PipelinedFallback.Cypher25.conf,
      TestConf.PipelinedFallback.Cypher5.FactoryName -> TestConf.PipelinedFallback.Cypher5.conf,
      TestConf.PipelinedNonFused.Cypher25.FactoryName -> TestConf.PipelinedNonFused.Cypher25.conf,
      TestConf.PipelinedNonFused.Cypher5.FactoryName -> TestConf.PipelinedNonFused.Cypher5.conf,
      TestConf.PipelinedRandMorselSize.Cypher25.FactoryName -> TestConf.PipelinedRandMorselSize.Cypher25.conf,
      TestConf.PipelinedRandMorselSize.Cypher5.FactoryName -> TestConf.PipelinedRandMorselSize.Cypher5.conf,
      TestConf.Parallel.Cypher25.FactoryName -> TestConf.Parallel.Cypher25.conf,
      TestConf.Parallel.Cypher5.FactoryName -> TestConf.Parallel.Cypher5.conf,
      TestConf.ParallelBolt.Cypher25.FactoryName -> TestConf.ParallelBolt.Cypher25.conf,
      TestConf.ParallelBolt.Cypher5.FactoryName -> TestConf.ParallelBolt.Cypher5.conf,
      TestConf.ParallelNonFused.Cypher25.FactoryName -> TestConf.ParallelNonFused.Cypher25.conf,
      TestConf.ParallelNonFused.Cypher5.FactoryName -> TestConf.ParallelNonFused.Cypher5.conf,
      TestConf.ParallelLeverageOrderNonFused.Cypher25.FactoryName -> TestConf.ParallelLeverageOrderNonFused.Cypher25.conf,
      TestConf.ParallelLeverageOrderNonFused.Cypher5.FactoryName -> TestConf.ParallelLeverageOrderNonFused.Cypher5.conf,
      TestConf.ParallelLeverageOrder.Cypher25.FactoryName -> TestConf.ParallelLeverageOrder.Cypher25.conf,
      TestConf.ParallelLeverageOrder.Cypher5.FactoryName -> TestConf.ParallelLeverageOrder.Cypher5.conf,
      TestConf.Slotted.Cypher25.FactoryName -> TestConf.Slotted.Cypher25.conf,
      TestConf.Slotted.Cypher5.FactoryName -> TestConf.Slotted.Cypher5.conf,
      TestConf.SlottedCompiled.Cypher25.FactoryName -> TestConf.SlottedCompiled.Cypher25.conf,
      TestConf.SlottedCompiled.Cypher5.FactoryName -> TestConf.SlottedCompiled.Cypher5.conf,
      TestConf.SlottedBolt.Cypher5.FactoryName -> TestConf.SlottedBolt.Cypher5.conf,
      TestConf.SlottedBolt.Cypher25.FactoryName -> TestConf.SlottedBolt.Cypher25.conf,
      TestConf.SpdBolt.FactoryName -> TestConf.SpdBolt.conf,
      TestConf.SpdParallel.FactoryName -> TestConf.SpdParallel.conf,
      TestConf.CommunityDefaultBolt.Cypher25.FactoryName -> TestConf.CommunityDefaultBolt.Cypher25.conf,
      TestConf.CommunityDefaultBolt.Cypher5.FactoryName -> TestConf.CommunityDefaultBolt.Cypher5.conf,
      TestConf.Legacy.FactoryName -> TestConf.Legacy.conf,
      TestConf.Planner.SmallIdpTableSize.FactoryName -> TestConf.Planner.SmallIdpTableSize.conf,
      TestConf.Planner.InferLabels.FactoryName -> TestConf.Planner.InferLabels.conf,
      TestConf.Planner.UpdateStrategyEager.FactoryName -> TestConf.Planner.UpdateStrategyEager.conf,
      TestConf.PlannerVersion.Experimental.Cypher25.FactoryName -> TestConf.PlannerVersion.Experimental.Cypher25.conf,
      TestConf.PlannerVersion.Experimental.Cypher5.FactoryName -> TestConf.PlannerVersion.Experimental.Cypher5.conf,
      TestConf.ReadOnlyUser.Cypher5.FactoryName -> TestConf.ReadOnlyUser.Cypher5.conf,
      TestConf.ReadOnlyUser.Cypher25.FactoryName -> TestConf.ReadOnlyUser.Cypher25.conf,
      ObfuscatorSteps.Conf.FactoryName -> ObfuscatorSteps.Conf.conf
    )

    // It's important that the ObjectFactoryName match the correct config.
    expected.foreach { case (className, conf) => withClue(className)(testConfs.get(className) shouldBe Some(conf)) }
    testConfs shouldBe expected

    // Additional smoke test
    val expectedPrefix = Map(
      classOf[CypherCucumberTest.TestConfiguration.ObjectFactory].getName -> "CYPHER runtime=legacy",
      TestConf.Default.Cypher25.FactoryName -> "",
      TestConf.Default.Cypher5.FactoryName -> "",
      TestConf.DefaultBolt.Cypher25.FactoryName -> "",
      TestConf.DefaultBolt.Cypher5.FactoryName -> "",
      TestConf.Pipelined.Cypher25.FactoryName -> "CYPHER runtime=pipelined",
      TestConf.Pipelined.Cypher5.FactoryName -> "CYPHER runtime=pipelined",
      TestConf.PipelinedFallback.Cypher25.FactoryName -> "CYPHER runtime=pipelined interpretedPipesFallback=all",
      TestConf.PipelinedFallback.Cypher5.FactoryName -> "CYPHER runtime=pipelined interpretedPipesFallback=all",
      TestConf.PipelinedNonFused.Cypher5.FactoryName -> "CYPHER runtime=pipelined operatorEngine=interpreted",
      TestConf.PipelinedNonFused.Cypher25.FactoryName -> "CYPHER runtime=pipelined operatorEngine=interpreted",
      TestConf.Parallel.Cypher25.FactoryName -> "CYPHER runtime=parallel",
      TestConf.Parallel.Cypher5.FactoryName -> "CYPHER runtime=parallel",
      TestConf.ParallelBolt.Cypher25.FactoryName -> "CYPHER runtime=parallel",
      TestConf.ParallelBolt.Cypher5.FactoryName -> "CYPHER runtime=parallel",
      TestConf.ParallelNonFused.Cypher5.FactoryName -> "CYPHER runtime=parallel operatorEngine=interpreted",
      TestConf.ParallelNonFused.Cypher25.FactoryName -> "CYPHER runtime=parallel operatorEngine=interpreted",
      TestConf.ParallelLeverageOrder.Cypher25.FactoryName -> "CYPHER runtime=parallel parallelRuntimeConfig=leverageOrder",
      TestConf.ParallelLeverageOrder.Cypher5.FactoryName -> "CYPHER runtime=parallel parallelRuntimeConfig=leverageOrder",
      TestConf.ParallelLeverageOrderNonFused.Cypher5.FactoryName -> "CYPHER runtime=parallel parallelRuntimeConfig=leverageOrder operatorEngine=interpreted",
      TestConf.ParallelLeverageOrderNonFused.Cypher25.FactoryName -> "CYPHER runtime=parallel parallelRuntimeConfig=leverageOrder operatorEngine=interpreted",
      TestConf.Slotted.Cypher25.FactoryName -> "CYPHER runtime=slotted",
      TestConf.Slotted.Cypher5.FactoryName -> "CYPHER runtime=slotted",
      TestConf.SlottedCompiled.Cypher25.FactoryName -> "CYPHER runtime=slotted expressionEngine=compiled",
      TestConf.SlottedCompiled.Cypher5.FactoryName -> "CYPHER runtime=slotted expressionEngine=compiled",
      TestConf.SlottedBolt.Cypher5.FactoryName -> "CYPHER runtime=slotted",
      TestConf.SlottedBolt.Cypher25.FactoryName -> "CYPHER runtime=slotted",
      TestConf.SpdBolt.FactoryName -> "",
      TestConf.SpdParallel.FactoryName -> "CYPHER runtime=parallel",
      TestConf.CommunityDefaultBolt.Cypher25.FactoryName -> "",
      TestConf.CommunityDefaultBolt.Cypher5.FactoryName -> "",
      TestConf.Legacy.FactoryName -> "CYPHER runtime=legacy",
      TestConf.Planner.SmallIdpTableSize.FactoryName -> "",
      TestConf.Planner.InferLabels.FactoryName -> "",
      TestConf.Planner.UpdateStrategyEager.FactoryName -> "CYPHER updateStrategy=eager"
    )
    expectedPrefix.foreach { case (className, prefix) =>
      withClue(className)(testConfs.get(className).map(_.preparserPrefix.trim) shouldBe Some(prefix))
    }
  }

  test("all planner versions introducing optimisations have corresponding PlannerVersion configs", Tags.NoSpdOverride) {
    val registeredFactoryNames = ServiceLoader.load(classOf[ObjectFactory]).stream().toList.asScala
      .map(p => p.`type`().getName)
      .toSet

    val versionsWithOptimisations = CypherPlannerVersionOption.supportedValues
      .filter(v => CypherPlannerVersionWithOptimisations.fromQueryOption(v).introducedOptimisations.nonEmpty)

    versionsWithOptimisations.foreach { version =>
      val objectName = version.toString.capitalize
      val pkg = "org.neo4j.cypher.cucumber.glue.regular"
      Seq("Cypher25", "Cypher5").foreach { cypherVariant =>
        val expectedName = s"$pkg.TestConf$$PlannerVersion$$$objectName$$$cypherVariant$$ObjectFactory"
        withClue(
          s"Missing feature test config for plannerVersion=$version ($cypherVariant). " +
            s"Add object $objectName to TestConf.PlannerVersion and register $expectedName in META-INF/services."
        ) {
          registeredFactoryNames should contain(expectedName)
        }
      }
    }
  }

  test("remember to add test coverage of the glue to avoid false positives", Tags.NoSpdOverride) {
    val covered = Set(
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.parametersAre(scala.collection.immutable.Map)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.givenCsvFile(java.lang.String,io.cucumber.datatable.DataTable)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.havingExecuted(java.lang.String)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.executingQuery(java.lang.String)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.registerUserFunction(java.lang.String)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.registerProcedure(java.lang.String,io.cucumber.datatable.DataTable)",
      "public default void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.registerProcedure(java.lang.String,io.cucumber.datatable.DataTable,scala.collection.immutable.Set)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.executingControlQuery(java.lang.String)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.approximateResultShouldBe(io.cucumber.datatable.DataTable,int)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.resultShouldBe(io.cucumber.datatable.DataTable,org.neo4j.cypher.cucumber.steps.Result$Assertions)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.sideEffectsShouldBe(io.cucumber.datatable.DataTable)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.errorShouldBeRaised(org.neo4j.cypher.cucumber.steps.CypherCucumberSteps$ExpectedGqlError)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.notificationsShouldBeRaised(org.neo4j.cypher.cucumber.steps.CypherCucumberSteps$ExpectedGqlNotification)",
      "public abstract void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.queryLogShouldContain(java.lang.String)",
      "public abstract void org.neo4j.cypher.cucumber.steps.InOpenTxCypherCucumberSteps.commitOpenTx()",
      "public abstract void org.neo4j.cypher.cucumber.steps.InOpenTxCypherCucumberSteps.executingControlQueryInOpenTx(java.lang.String)",
      "public abstract void org.neo4j.cypher.cucumber.steps.InOpenTxCypherCucumberSteps.openTransaction()",
      "public abstract void org.neo4j.cypher.cucumber.steps.InOpenTxCypherCucumberSteps.havingExecutedInOpenTx(java.lang.String)",
      "public abstract void org.neo4j.cypher.cucumber.steps.InOpenTxCypherCucumberSteps.executingQueryInOpenTx(java.lang.String)",
      "public default void org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.loadNamedGraph(java.lang.String)"
    )
    val methods = classOf[CypherCucumberSteps].getMethods
      .filter(c => c.toString.contains("org.neo4j.cypher.cucumber"))
      .filter(c => !Modifier.isStatic(c.getModifiers))
      .map(_.toString)
      .toSet
    if (methods != covered) {
      fail(
        s"""
           |You might want to add test coverage of the following methods to avoid false positives:
           |
           |${methods.diff(covered).mkString("\n")}
           |
           |Could not find these:
           |${covered.diff(methods).mkString("\n")}
           |""".stripMargin
      )
    }
  }

  def wrongResultOrdered(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable)
      .hasMessageContainingAll(
        "Incorrect query result.",
        "Expected results (in order):",
        "CYPHER runtime=legacy",
        "+ProduceResults"
      )
  }

  def wrongResultAnyOrder(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable)
      .hasMessageContainingAll(
        "Incorrect query result.",
        "Expected results (in any order):",
        "CYPHER runtime=legacy",
        "+ProduceResults"
      )
  }

  def wrongResultOrderedAnyListOrder(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable)
      .hasMessageContainingAll(
        "Incorrect query result.",
        "Expected results (in order):",
        "runtime=legacy",
        "+ProduceResults"
      )
  }

  def wrongResultAnyOrderAnyListOrder(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable)
      .hasMessageContainingAll(
        "Incorrect query result.",
        "Expected results (in any order):",
        "CYPHER runtime=legacy",
        "+ProduceResults"
      )
  }

  def wrongHeaders(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContainingAll("Result has correct headers", "+ProduceResults")
  }

  def wrongSideEffects(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContainingAll(
      "Incorrect side effects",
      "Actual side effects:",
      "+ProduceResults"
    )
  }

  def queryFailedCompile(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable)
      .hasMessageContainingAll(
        "Query failed but was expected to succeed.",
        "Phase: compile",
        "CYPHER runtime=legacy"
      )
  }

  def queryFailedRuntime(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable)
      .hasMessageContainingAll("Query failed but was expected to succeed.", "Phase: runtime", "CYPHER runtime=legacy")
  }

  def wrongGqlCode(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContainingAll(
      "Expected GQL status",
      "Cause: org.neo4j.graphdb.QueryExecutionException: Invalid input"
    )
  }

  def wrongFailureMessage(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContainingAll(
      "Actual errors:",
      "Did not match expected errors:"
    )
  }

  def wrongWarningGqlCode(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContainingAll(
      "Actual warnings",
      "Did not match expected warnings:"
    )
  }

  def wrongWarningMessage(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContainingAll(
      "Incorrect message",
      "warn: feature deprecated with replacement."
    )
  }

  def wrongConf(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContaining("make.sure.im.there.*")
  }

  def wrongConfTag(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(
      failure.throwable
    ).hasMessageContaining("Unrecognized setting. No declared setting with name: incorrect.conf")
  }

  def wrongNbrResults(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContaining("but was")
  }

  def wrongApproximateMandatoryColumn(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(
      failure.throwable
    ).hasMessageContaining("Expected the last column in an approximate result to be named 'mandatory'")
  }

  def wrongApproximateMandatoryType(name: String): Consumer[CypherCucumberTest.Failure] = failure => {
    assertThat(failure.testName).isEqualTo("TestFrameworkTests - " + name)
    assertThat(failure.throwable).hasMessageContaining("Expected last column to contain only booleans, but found")
  }
}

object CypherCucumberTest {

  object TestConfiguration extends InjectedTestConf {

    final override val conf: TestConf = TestConf(
      neo4jConf = Map(
        "dbms.security.procedures.unrestricted" -> "make.sure.im.there.*"
      ),
      useEnterprise = false,
      preparserOptions = Map("runtime" -> "legacy")
    )
    final class ObjectFactory extends SingletonInjector(injector)
  }

  case class Failure(testName: String, throwable: Throwable) {

    override def toString: String =
      s"Failure(testName = $testName, throwable: ${throwable.getClass.getSimpleName})"
  }

  class TestListener extends TestExecutionListener {
    val passing: java.util.List[String] = Collections.synchronizedList(new util.ArrayList[String]())

    override def executionFinished(id: TestIdentifier, result: TestExecutionResult): Unit = result.getStatus match {
      case TestExecutionResult.Status.SUCCESSFUL => if (id.isTest) passing.add(id.getDisplayName)
      case _                                     =>
    }
  }

}
