/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Selectivity
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

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

}
