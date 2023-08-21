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
package org.neo4j.cypher.internal.ast.generator

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Gen
import org.scalacheck.Shrink

class AstGeneratorTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  private val astGenerator = new AstGenerator()

  // Avoid shrinking as that give invalid continued generation instead of aborting on errors
  def noShrink[T]: Shrink[T] = Shrink[T](_ => Stream.empty)
  implicit val intListNoShrink: Shrink[List[Int]] = noShrink[List[Int]]
  implicit val intNoShrink: Shrink[Int] = noShrink[Int]

  test("listSetOfSizeBetween") {
    val g = for {
      min <- Gen.choose(1, 10)
      max <- Gen.choose(10, 20)
      list <- AstGenerator.listSetOfSizeBetween(min, max, Gen.choose(0, max))
    } yield (min, max, list)

    forAll(g) { case (min, max, list) =>
      list.size should be >= min
      list.size should be <= max
    }
  }

  test("_predicateComparisonChain") {
    forAll(astGenerator._predicateComparisonChain) {

      case Ands(comparisons) =>
        comparisons.size match {
          case size if size < 2 =>
            fail(s"Expected at least 2 comparisons but was $size")
          case size if size > 4 =>
            fail(s"Expected at most 4 comparisons but was $size")
          case _ =>
        }
      case x => fail(s"Expected Ands(exprs) but was ${x.getClass}")
    }
  }
}
