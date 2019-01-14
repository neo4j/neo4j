/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.cardinality

import org.neo4j.cypher.internal.v3_5.util.Selectivity
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class SelectivityCombinerTest extends CypherFunSuite {

  test("should not lose precision for intermediate numbers") {
    val selectivities = Seq(Selectivity.of(1e-10).get, Selectivity.of(2e-10).get)

    IndependenceCombiner.orTogetherSelectivities(selectivities).get should not equal Selectivity.ZERO
  }

  test("should not lose precision for small numbers") {
    val selectivities = Seq(Selectivity.of(1e-100).get, Selectivity.of(2e-100).get, Selectivity.of(1e-300).get)

    IndependenceCombiner.orTogetherSelectivities(selectivities).get should not equal Selectivity.ZERO
  }

  test("ANDing together works as expected") {
    val selectivities = Seq(Selectivity.of(.1).get, Selectivity.of(.2).get, Selectivity.ONE)

    val selectivity = IndependenceCombiner.andTogetherSelectivities(selectivities).get.factor
    assert( selectivity === 0.02 +- 0.000000000000000002 )
  }

  test("ORing together works as expected") {
    val selectivities = Seq(Selectivity.of(.1).get, Selectivity.of(.2).get)

    val selectivity = IndependenceCombiner.orTogetherSelectivities(selectivities).get.factor
    assert( selectivity === 0.28 +- 0.000000000000000028 )
  }

  test("OR: size 1") {
    val a = 0.3
    IndependenceCombiner.orTogetherSelectivities(Seq(a).map(Selectivity(_))).map(_.factor).get should be(a)
  }

  test("OR: size 2") {
    val a = 0.3
    val b = 0.85
    IndependenceCombiner.orTogetherSelectivities(Seq(a, b).map(Selectivity(_))).map(_.factor).get should be(
      a + b
        - a * b
        +- 0.001
    )
  }

  test("OR: size 3") {
    val a = 0.3
    val b = 0.85
    val c = 0.077
    IndependenceCombiner.orTogetherSelectivities(Seq(a, b, c).map(Selectivity(_))).map(_.factor).get should be(
      a + b + c
        - a * b - a * c - b * c
        + a * b * c
        +- 0.001
    )
  }

  test("OR: size 4") {
    val a = 0.3
    val b = 0.85
    val c = 0.077
    val d = 0.935489
    IndependenceCombiner.orTogetherSelectivities(Seq(a, b, c, d).map(Selectivity(_))).map(_.factor).get should be(
      a + b + c + d
        - a * b - a * c - a * d - b * c - b * d - c * d
        + a * b * c + a * b * d + a * c * d + b * c * d
        - a * b * c * d
        +- 0.001
    )
  }

}
