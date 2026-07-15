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
package org.neo4j.cypher.internal.frontend.prettifier

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.generator.AstGenerator
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks

class PrettifierPropertyTest extends CypherFunSuite
    with CypherScalaCheckDrivenPropertyChecks
    with PrettifierTestUtils {

  val prettifier: Prettifier =
    Prettifier(ExpressionStringifier(alwaysParens = true, alwaysBacktick = true, sensitiveParamsAsParams = true))

  // Show constraints changed ASTs, prettified version and parsing rules between Cypher 5 and Cypher 25
  // so doing the round trip with a parser that doesn't match the generated AST would fail with parsing exceptions
  val astGeneratorCypher5 =
    new AstGenerator(simpleStrings = false, whenAstDifferUseCypherVersion = CypherVersion.Cypher5)

  val astGeneratorCypher25 =
    new AstGenerator(simpleStrings = false, whenAstDifferUseCypherVersion = CypherVersion.Cypher25)

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(
    minSuccessful = 10000,
    // AstGenerator limits AST depth with the size parameter to try to avoid stack overflows
    minSize = 4,
    sizeRange = 12
  )

  test("Prettifier output should parse to the same ast - Cypher 5 version") {
    // To reproduce test failures, enable the following line with the seed from the TC build
    // setScalaCheckInitialSeed(seed)
    forAll(astGeneratorCypher5._statement) { statement =>
      roundTripCheck(statement, CypherVersion.Cypher5)
    }
  }

  test("Prettifier output should parse to the same ast - Cypher 25 version") {
    // To reproduce test failures, enable the following line with the seed from the TC build
    // setScalaCheckInitialSeed(seed)
    forAll(astGeneratorCypher25._statement) { statement =>
      roundTripCheck(statement, CypherVersion.Cypher25)
    }
  }
}
