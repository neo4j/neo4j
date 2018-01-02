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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LazyIterableTest extends CypherFunSuite {

  test("iterates") {
    LazyIterable(Seq(1, 2, 3).iterator).toSeq should equal(Seq(1, 2, 3))
  }

  test("iterates lazily") {
    var start = 10
    val iterable = LazyIterable {
      Seq(start, start + 1, start + 2).iterator
    }

    iterable.toSeq should equal (Seq(10, 11, 12))

    start = 20

    iterable.toSeq should equal (Seq(20, 21, 22))
  }
}
