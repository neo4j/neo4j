/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.opencypher.tools.tck.api.Scenario

import java.util

import scala.collection.JavaConverters.asJavaCollectionConverter

/**
 * Use this class to run tests for Cucumber scenarios.
 */
@Execution(ExecutionMode.CONCURRENT)
abstract class FeatureTest {

  /**
   * @return the scenarios to run all tests for.
   */
  def scenarios: Seq[Scenario]

  /**
   * @return the scenarios which are expected to fail.
   */
  def denylist: Seq[DenylistEntry]

  /**
   * Invoked for each denylisted scenario.
   *
   * @return Executables that will be turned into [[DynamicTest]]s.
   */
  def runDenyListedScenario(scenario: Scenario): Seq[Executable]

  /**
   * Invoked for each non-denylisted scenario.
   *
   * @return Executables that will be turned into [[DynamicTest]]s.
   */
  def runScenario(scenario: Scenario): Seq[Executable]

  @TestFactory
  def runTests(): util.Collection[DynamicTest] = {
    val (expectFail, expectPass) = scenarios.partition(s => denylist.exists(_.isDenylisted(s)))

    val expectFailTests: Seq[DynamicTest] = for {
      scenario <- expectFail
      name = scenario.toString()
      executable <- runDenyListedScenario(scenario)
    } yield DynamicTest.dynamicTest("Denylisted Test: " + name, executable)

    val expectPassTests: Seq[DynamicTest] = for {
      scenario <- expectPass
      name = scenario.toString()
      executable <- runScenario(scenario)
    } yield DynamicTest.dynamicTest(name, executable)
    (expectPassTests ++ expectFailTests).asJavaCollection
  }
}
