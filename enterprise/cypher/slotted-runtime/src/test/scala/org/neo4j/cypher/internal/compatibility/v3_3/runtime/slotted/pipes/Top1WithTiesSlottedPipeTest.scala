/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes.TopSlottedPipeTestSupport._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class Top1WithTiesSlottedPipeTest extends CypherFunSuite {

  test("empty input gives empty output") {
    val result = singleColumnTop1WithTiesWithInput(List(), orderBy = AscendingOrder)
    result should be(empty)
  }

  test("simple sorting works as expected") {
    val input = List("B", "A")
    val result = singleColumnTop1WithTiesWithInput(input, orderBy = AscendingOrder)
    result should equal(list("A"))
  }

  test("two ties for the first place are all returned") {
    val input = List(
      (1, 1),
      (1, 2),
      (2, 3),
      (2, 4)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      (1, 1),
      (1, 2)
    ))
  }

  test("if only null is present, it should be returned") {
    val input = List(
      (null, 1),
      (null, 2)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      (null, 1),
      (null, 2)
    ))
  }

  test("null should not be returned if other values are present") {
    val input = List(
      (1, 1),
      (null, 2),
      (2, 3)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      (1, 1)
    ))
  }

  test("comparing arrays") {
    val smaller = Array(1, 2)
    val input = List(
      (Array(3,4), 2),
      (smaller, 1)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      (smaller, 1)
    ))
  }

  test("comparing numbers and strings") {
    val input = List(
      (1, 1),
      ("A", 2)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      ("A", 2)
    ))
  }
}
