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
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class AstGeneratorTest extends CypherFunSuite with ScalaCheckDrivenPropertyChecks {

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
