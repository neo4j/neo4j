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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AnonymousVariableNameGeneratorTest extends CypherFunSuite {

  test("genName minted names for a mix of named, already-anon, and unusual inputs") {
    Seq(
      // already-anon names pass through as the next raw anon name
      ("  UNNAMED1", "  UNNAMED0"),
      // existing deduped names are re-minted from scratch at counter 0
      ("  d@0", "  d@0"),
      ("  d@42", "  d@0"),
      // a simple named variable is rendered with the "@counter" suffix
      ("d", "  d@0"),
      // weird characters in the original name are preserved verbatim
      ("some weird propy38259dsyfj name", "  some weird propy38259dsyfj name@0")
    ).foreach { case (input, expected) =>
      AnonymousVariableNameGenerator.genName(new AnonymousVariableNameGenerator, input) should equal(expected)
    }
  }

  test("includeName replaces the UNNAMED prefix with <name>@ for named variables") {
    AnonymousVariableNameGenerator.includeName("foo", "  UNNAMED7") should equal("  foo@7")
  }

  test("includeName returns the raw anon name for already-anon inputs") {
    AnonymousVariableNameGenerator.includeName("  UNNAMED3", "  UNNAMED7") should equal("  UNNAMED7")
  }

  test("genName advances the counter per call") {
    val gen = new AnonymousVariableNameGenerator
    AnonymousVariableNameGenerator.genName(gen, "x") should equal("  x@0")
    AnonymousVariableNameGenerator.genName(gen, "x") should equal("  x@1")
    AnonymousVariableNameGenerator.genName(gen, "y") should equal("  y@2")
  }
}
