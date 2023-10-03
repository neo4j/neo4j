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

  val astGenerator = new AstGenerator(simpleStrings = false)

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 5000)

  test("Prettifier output should parse to the same ast") {
    assume(Runtime.version().feature() < 21)
    // To reproduce test failures, enable the following line with the seed from the TC build
    // setScalaCheckInitialSeed(seed)
    forAll(astGenerator._statement) { statement =>
      roundTripCheck(statement)
    }
  }
}
