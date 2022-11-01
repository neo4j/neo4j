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
package org.neo4j.cypher.internal.ast.generator

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class AstGeneratorTest extends CypherFunSuite with GeneratorDrivenPropertyChecks {

  private val astGenerator = new AstGenerator()

  test("listOfSizeBetween") {
    val g = for {
      min <- Gen.choose(1, 10)
      max <- Gen.choose(10, 20)
      list <- AstGenerator.listOfSizeBetween(min, max, Gen.const(123))
    } yield (min, max, list)

    forAll(g) { case (min, max, list) =>
      list.size should be >= min
      list.size should be <= max
    }
  }

  test("_predicateComparisonChain") {
    forAll(astGenerator._predicateComparisonChain) {
      case Ands(exprs) =>
        exprs.size should be > 1
    }
  }
}
