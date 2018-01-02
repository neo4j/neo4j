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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.scalatest.FunSuite

class NiceHasherTest extends CypherFunSuite {

  test("compares arrays by their contents rather than object identity") {
    val hasher1 = new NiceHasher(Seq(Array(1, 2, 3)))
    val hasher2 = new NiceHasher(Seq(Array(1, 2, 3)))
    hasher1 should equal(hasher2)
  }

  test("should work when nice hasher wraps null") {
    new NiceHasher(null) should equal(new NiceHasher(null))
    new NiceHasher(Seq.empty) should not equal new NiceHasher(null)
    new NiceHasher(null) should not equal new NiceHasher(Seq.empty)
  }

  test("should work when nice hasher wraps seq of nulls") {
    val hasher1 = new NiceHasher(Seq(null, null, null))
    val hasher2 = new NiceHasher(Seq(null, null, null))
    hasher1 should equal(hasher2)
  }
}
