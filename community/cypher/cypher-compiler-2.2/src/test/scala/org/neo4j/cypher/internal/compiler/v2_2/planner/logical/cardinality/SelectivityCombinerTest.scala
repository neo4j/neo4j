/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Selectivity

class SelectivityCombinerTest extends CypherFunSuite {

  test("should not lose precision for intermediate numbers") {
    val selectivities = Seq(Selectivity(1e-10), Selectivity(2e-10))

    IndependenceCombiner.orTogetherSelectivities(selectivities).get should not equal Selectivity(0)
  }

  test("should not lose precision for small numbers") {
    val selectivities = Seq(Selectivity(1e-100), Selectivity(2e-100), Selectivity(1e-300))

    IndependenceCombiner.orTogetherSelectivities(selectivities).get should not equal Selectivity(0)
  }

  test("ANDing together works as expected") {
    val selectivities = Seq(Selectivity(.1), Selectivity(.2), Selectivity.ONE)

    IndependenceCombiner.andTogetherSelectivities(selectivities).get should equal(Selectivity(0.02))
  }

  test("ORing together works as expected") {
    val selectivities = Seq(Selectivity(.1), Selectivity(.2))

    IndependenceCombiner.orTogetherSelectivities(selectivities).get should equal(Selectivity(0.28))
  }

}
