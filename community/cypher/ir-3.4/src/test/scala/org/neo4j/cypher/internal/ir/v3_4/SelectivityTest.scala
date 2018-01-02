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
package org.neo4j.cypher.internal.ir.v3_4

import org.neo4j.cypher.internal.util.v3_4.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class SelectivityTest extends CypherFunSuite {

  test("negating a selectivity behaves as expected") {
    Selectivity.of(.1).get.negate should not equal Selectivity.ONE
    Selectivity.of(5.6e-17).get.negate should not equal Selectivity.ONE
    Selectivity.of(1e-300).get.negate should not equal Selectivity.ONE

    Selectivity.ZERO.negate should equal(Selectivity.ONE)
    Selectivity.ZERO.negate should equal(Selectivity.ONE)

    Selectivity.ONE.negate should equal(Selectivity.ZERO)
    Selectivity.ONE.negate should equal(Selectivity.ZERO)

    Selectivity.CLOSEST_TO_ONE.negate should not equal Selectivity.ONE
  }

  test("selectivity and cardinality should not be able to produce NaN or Infinity through multiplication") {
    ( Cardinality(Double.PositiveInfinity) * Selectivity.ZERO ) should equal ( Cardinality.EMPTY )

    val maxCardinality = Cardinality(Double.MaxValue)
    ( Cardinality(Double.PositiveInfinity) * Cardinality.SINGLE ) should equal ( maxCardinality )
    ( Cardinality(Double.PositiveInfinity) * Cardinality(12) ) should equal ( maxCardinality )
    ( maxCardinality * Cardinality(1.00001) ) should equal ( maxCardinality )

    ( Cardinality(3223143) ^ 50 ) should equal ( maxCardinality )
  }
}
