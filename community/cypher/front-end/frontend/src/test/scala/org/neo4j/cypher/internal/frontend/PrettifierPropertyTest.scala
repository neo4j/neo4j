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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.generator.AstGenerator
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class PrettifierPropertyTest extends CypherFunSuite
  with GeneratorDrivenPropertyChecks
  with PrettifierTestUtils {

  val prettifier: Prettifier = Prettifier(ExpressionStringifier(alwaysParens = true, alwaysBacktick = true, sensitiveParamsAsParams = true))

  val astGenerator = new AstGenerator(simpleStrings = false)

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 5000)

  test("Prettifier output should parse to the same ast") {
    forAll(astGenerator._statement) { statement =>
      roundTripCheck(statement)
    }
  }
}
