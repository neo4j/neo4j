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
package org.neo4j.cypher.internal.frontend.scoping

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25
import org.scalatest.exceptions.TestFailedException

/**
 * Unit tests for the [[Outcome]] assertion machinery itself ([[check]] and the `AllOf` / `Absent` /
 * `Exactly` combinators), as opposed to the per-error `GQL_*` suites which only ever exercise it on
 * the passing path. Each combinator is checked in BOTH directions — that it passes when it should and
 * fails (throws) when it should — so a bug that silently weakens an assertion is caught.
 */
class OutcomeAssertionTest extends VariableCheckingTestSuite {

  // `RETURN x` — `x` is undefined: produces exactly {42N62}.
  private val singleError = "RETURN x"
  // `x` undefined (42N62) AND two return items named `a` (42N38): produces exactly {42N62, 42N38}.
  private val twoErrors = "RETURN x AS a, 1 AS a"

  private val v25 = Array(CypherVersion.Cypher25)

  // Asserts that `check` rejects `outcome` for `query` (i.e. the assertion itself fails).
  private def rejects(query: String, outcome: Outcome): Unit =
    intercept[TestFailedException](check(query, outcome, v25))

  // ----- Exactly: leaf-code-set equality + per-code message -----

  test("Exactly accepts the single produced code") {
    check(singleError, Exactly(E42N62("x")), v25)
  }

  test("Exactly accepts the exact set of produced codes") {
    check(twoErrors, Exactly(E42N62("x"), E42N38), v25)
  }

  test("Exactly rejects a missing expected code") {
    rejects(singleError, Exactly(E42N62("x"), E42N07("y")))
  }

  test("Exactly rejects an extra produced code") {
    rejects(twoErrors, Exactly(E42N62("x")))
  }

  test("Exactly rejects a matching code with the wrong message") {
    rejects(singleError, Exactly(E42N62("zzz")))
  }

  // ----- Absent: chain-containment -----

  test("Absent accepts a code that was not produced") {
    check(singleError, Absent("42N07"), v25)
  }

  test("Absent rejects a code that was produced") {
    rejects(singleError, Absent("42N62"))
  }

  // ----- AllOf: composition -----

  test("AllOf accepts a present code alongside an absent one") {
    check(singleError, AllOf(E42N62("x"), Absent("42N07")), v25)
  }

  test("AllOf rejects when a composed expectation fails") {
    rejects(singleError, AllOf(E42N62("x"), E42N07("y")))
  }

  // ----- Pre-existing outcomes (previously untested) -----

  test("Passes accepts a query with no errors") {
    check("RETURN 1 AS a", Passes, v25)
  }

  test("A single GqlError is matched by code and message") {
    check(singleError, E42N62("x"), v25)
  }

  test("Versioned dispatches per version (Cypher 5 ignored, Cypher 25 asserted)") {
    check(singleError, ignoreBeforeCypher25(E42N62("x")), CypherVersion.values())
  }

  // ----- relaxedForFuzzing: weakening exact assertions for surrounded/fuzzed queries -----

  test("relaxedForFuzzing rewrites Exactly to AllOf of the same errors") {
    Outcome.relaxedForFuzzing(Exactly(E42N62("x"), E42N38)) shouldBe AllOf(E42N62("x"), E42N38)
  }

  test("relaxedForFuzzing rewrites Absent to Ignore") {
    Outcome.relaxedForFuzzing(Absent("42N62")) shouldBe Ignore
  }

  test("relaxedForFuzzing leaves a single GqlError, Passes and Ignore unchanged") {
    Outcome.relaxedForFuzzing(E42N62("x")) shouldBe E42N62("x")
    Outcome.relaxedForFuzzing(Passes) shouldBe Passes
    Outcome.relaxedForFuzzing(Ignore) shouldBe Ignore
  }

  test("relaxedForFuzzing recurses into AllOf, relaxing each element") {
    Outcome.relaxedForFuzzing(AllOf(Exactly(E42N62("x")), Absent("42N38"), E42N07("y"))) shouldBe
      AllOf(AllOf(E42N62("x")), Ignore, E42N07("y"))
  }

  test("relaxedForFuzzing recurses into Versioned, relaxing the default and every branch") {
    Outcome.relaxedForFuzzing(
      Versioned(Exactly(E42N62("x")), CypherVersion.Cypher5 -> Absent("42N38"))
    ) shouldBe
      Versioned(AllOf(E42N62("x")), CypherVersion.Cypher5 -> Ignore)
  }

  test("Exactly rejects an extra produced code, but its relaxed form tolerates it") {
    rejects(twoErrors, Exactly(E42N62("x")))
    check(twoErrors, Outcome.relaxedForFuzzing(Exactly(E42N62("x"))), v25)
  }

  test("Absent rejects a produced code, but its relaxed form (Ignore) tolerates it") {
    rejects(singleError, Absent("42N62"))
    check(singleError, Outcome.relaxedForFuzzing(Absent("42N62")), v25)
  }
}
