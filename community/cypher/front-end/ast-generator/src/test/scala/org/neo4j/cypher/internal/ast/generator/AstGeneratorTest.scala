/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
