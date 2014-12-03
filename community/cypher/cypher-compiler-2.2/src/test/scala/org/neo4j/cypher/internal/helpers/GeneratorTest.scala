/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.helpers

import org.neo4j.cypher.internal.commons.CypherFunSuite

class GeneratorTest extends CypherFunSuite {

  test("Should generate values") {
    TestGenerator(3).toList should equal(List(1, 2, 3))
  }

  test("Should generate no values") {
    TestGenerator(0).toList should equal(List())
  }

  test("Should fail if empty") {
    evaluating {
      val iter = TestGenerator(0).iterator
      iter.next()
    } should produce[NoSuchElementException]
  }

  test("Should handle multiple calls to hasNext") {
    val iter = TestGenerator(3).iterator

    iter.next() should equal(1)

    iter.hasNext should equal(true)
    iter.hasNext should equal(true)

    iter.next() should equal(2)

    iter.hasNext should equal(true)
    iter.hasNext should equal(true)
    iter.hasNext should equal(true)

    iter.next() should equal(3)

    iter.hasNext should equal(false)
    iter.hasNext should equal(false)
    iter.hasNext should equal(false)
  }

  final case class TestGenerator(maxCount: Int) extends Generator[Int] {
    var deliverNext: Int = 0

    def fetchNext: DeliveryState = {
      if (deliverNext < maxCount) {
        deliverNext += 1
        return ReadyToDeliver
      }
      NothingToDeliver
    }
  }
}
