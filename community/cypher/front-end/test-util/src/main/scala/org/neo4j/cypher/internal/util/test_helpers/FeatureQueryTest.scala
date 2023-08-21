/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import org.junit.jupiter.api.function.Executable
import org.opencypher.tools.tck.api.Execute
import org.opencypher.tools.tck.api.Scenario

/**
 * Use this trait when you only need the query text to run your test instead of the whole scenario.
 */
trait FeatureQueryTest extends FeatureTest {

  /**
   * Invoked for each query in denylisted scenarios.
   *
   * @param query the query text
   * @return optionally an Executable that will be turned into a [[org.junit.jupiter.api.DynamicTest]]
   */
  def runDenyListedQuery(scenario: Scenario, query: String): Option[Executable]

  /**
   * Invoked for each query in non-denylisted scenarios.
   *
   * @param query the query text
   * @return optionally an Executable that will be turned into a [[org.junit.jupiter.api.DynamicTest]]
   */
  def runQuery(scenario: Scenario, query: String): Option[Executable]

  final override def runDenyListedScenario(scenario: Scenario): Seq[Executable] =
    getQueries(scenario).flatMap(runDenyListedQuery(scenario, _))

  final override def runScenario(scenario: Scenario): Seq[Executable] =
    getQueries(scenario).flatMap(runQuery(scenario, _))

  private def getQueries(scenario: Scenario): Seq[String] = {
    scenario.steps.collect {
      case Execute(query, _, _) => query
    }
  }
}
