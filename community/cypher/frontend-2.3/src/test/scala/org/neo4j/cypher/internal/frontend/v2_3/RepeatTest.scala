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
package org.neo4j.cypher.internal.frontend.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class RepeatTest extends CypherFunSuite {

  var count = 0
  val result = new Object
  val mockedRewriter = new Rewriter {
    def apply(v1: AnyRef): AnyRef = {
      count += 1
      result
    }
  }

  test("should not repeat when the output is the same of the input") {
    // given
    count = 0

    // when
    val output = repeat(mockedRewriter)(result)

    // then
    output should equal(result)
    count should equal(1)
  }

  test("should repeat once when the output is different from the input") {
    // given
    count = 0

    // when
    val output = repeat(mockedRewriter)(new Object)

    // then
    output should equal(result)
    count should equal(2)
  }
}
