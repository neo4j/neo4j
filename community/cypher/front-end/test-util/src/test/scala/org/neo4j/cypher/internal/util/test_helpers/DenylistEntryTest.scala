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

class DenylistEntryTest extends CypherFunSuite {

  test("parseSimpleScenario") {
    DenylistEntry(
      """Feature "QuantifiedPathPatternAcceptance": Scenario "Solutions can be assigned to path variable""""
    ) should equal(
      ScenarioDenylistEntry(
        Some("QuantifiedPathPatternAcceptance"),
        "Solutions can be assigned to path variable",
        None,
        isFlaky = false,
        acceptTransientError = false
      )
    )
  }

  test("parseExampleScenario") {
    DenylistEntry(
      """Feature "List11 - Create a list from a range": Scenario "Create list from `range()` with explicitly given step": Example "10""""
    ) should equal(
      ScenarioDenylistEntry(
        Some("List11 - Create a list from a range"),
        "Create list from `range()` with explicitly given step",
        Some("10"),
        isFlaky = false,
        acceptTransientError = false
      )
    )
  }

  test("parseFlakyScenario") {
    DenylistEntry(
      """?Feature "QuantifiedPathPatternAcceptance": Scenario "Solutions can be assigned to path variable""""
    ) should equal(
      ScenarioDenylistEntry(
        Some("QuantifiedPathPatternAcceptance"),
        "Solutions can be assigned to path variable",
        None,
        isFlaky = true,
        acceptTransientError = false
      )
    )
  }

  test("parseAcceptTransientErrorScenario") {
    DenylistEntry(
      """~Feature "QuantifiedPathPatternAcceptance": Scenario "Solutions can be assigned to path variable""""
    ) should equal(
      ScenarioDenylistEntry(
        Some("QuantifiedPathPatternAcceptance"),
        "Solutions can be assigned to path variable",
        None,
        isFlaky = false,
        acceptTransientError = true
      )
    )
  }

  test("parseFlakyAcceptTransientErrorScenario") {
    DenylistEntry(
      """?~Feature "QuantifiedPathPatternAcceptance": Scenario "Solutions can be assigned to path variable""""
    ) should equal(
      ScenarioDenylistEntry(
        Some("QuantifiedPathPatternAcceptance"),
        "Solutions can be assigned to path variable",
        None,
        isFlaky = true,
        acceptTransientError = true
      )
    )
  }

  test("parseFeatureOnly") {
    DenylistEntry("""Feature "QuantifiedPathPatternAcceptance"""") should equal(
      FeatureDenylistEntry("QuantifiedPathPatternAcceptance", isFlaky = false, acceptTransientError = false)
    )
  }

  test("parseFlakyFeatureOnly") {
    DenylistEntry("""?Feature "QuantifiedPathPatternAcceptance"""") should equal(
      FeatureDenylistEntry("QuantifiedPathPatternAcceptance", isFlaky = true, acceptTransientError = false)
    )
  }

  test("parseAcceptTransientErrorFeatureOnly") {
    DenylistEntry("""~Feature "QuantifiedPathPatternAcceptance"""") should equal(
      FeatureDenylistEntry("QuantifiedPathPatternAcceptance", isFlaky = false, acceptTransientError = true)
    )
  }

  test("parseFlakyAcceptTransientErrorFeatureOnly") {
    DenylistEntry("""?~Feature "QuantifiedPathPatternAcceptance"""") should equal(
      FeatureDenylistEntry("QuantifiedPathPatternAcceptance", isFlaky = true, acceptTransientError = true)
    )
  }

  test("toStringFeatureOnly") {
    FeatureDenylistEntry(
      "QuantifiedPathPatternAcceptance",
      isFlaky = false,
      acceptTransientError = false
    ).toString should equal(
      """Feature "QuantifiedPathPatternAcceptance""""
    )
  }

  test("toStringFlakyFeatureOnly") {
    FeatureDenylistEntry(
      "QuantifiedPathPatternAcceptance",
      isFlaky = true,
      acceptTransientError = false
    ).toString should equal(
      """?Feature "QuantifiedPathPatternAcceptance""""
    )
  }

  test("toStringAcceptTransientErrorFeatureOnly") {
    FeatureDenylistEntry(
      "QuantifiedPathPatternAcceptance",
      isFlaky = false,
      acceptTransientError = true
    ).toString should equal(
      """~Feature "QuantifiedPathPatternAcceptance""""
    )
  }

  test("toStringFlakyAcceptTransientErrorFeatureOnly") {
    FeatureDenylistEntry(
      "QuantifiedPathPatternAcceptance",
      isFlaky = true,
      acceptTransientError = true
    ).toString should equal(
      """?~Feature "QuantifiedPathPatternAcceptance""""
    )
  }

  test("toStringExample") {
    ScenarioDenylistEntry(
      Some("List11 - Create a list from a range"),
      "Create list from `range()` with explicitly given step",
      Some("10"),
      isFlaky = true,
      acceptTransientError = true
    ).toString should equal(
      """?~Feature "List11 - Create a list from a range": Scenario "Create list from `range()` with explicitly given step": Example "10""""
    )

  }
}
