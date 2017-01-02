/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.idp

import org.neo4j.cypher.internal.commons.CypherFunSuite

import scala.collection.immutable.BitSet

class IdRegistryTest extends CypherFunSuite {

  test("IdRegistry registers elements starting from 0") {
    val registry = IdRegistry[Symbol]

    registry.register('a) should equal(0)
    registry.register('a) should equal(0)
    registry.register('b) should equal(1)
    registry.register('a) should equal(0)
    registry.register('b) should equal(1)
  }

  test("IdRegistry can compact registered ids into new fresh ids") {
    val registry = IdRegistry[Symbol]

    registry.register('a) should equal(0)
    registry.compact(BitSet(0)) should equal(1)
    registry.register('a) should equal(0)
    registry.register('b) should equal(2)
  }

  test("IdRegistry can register whole sets") {
    val registry = IdRegistry[Symbol]

    registry.registerAll(Set('a, 'b)) should equal(BitSet(0, 1))
    registry.registerAll(Set('b, 'c)) should equal(BitSet(1, 2))
  }

  test("IdRegistry translates back while honoring previous compactions") {
    val registry = IdRegistry[Symbol]

    registry.register('a)
    registry.compact(BitSet(0))
    registry.registerAll(Set('a, 'b, 'c))

    registry.lookup(0) should equal(Some('a))
    registry.lookup(1) should equal(None)
    registry.lookup(2) should equal(Some('b))
    registry.lookup(3) should equal(Some('c))

    registry.explode(BitSet(0, 2, 3)) should equal(Set('a, 'b, 'c))
    registry.explode(BitSet(1, 2, 3)) should equal(Set('a, 'b, 'c))
  }
}
