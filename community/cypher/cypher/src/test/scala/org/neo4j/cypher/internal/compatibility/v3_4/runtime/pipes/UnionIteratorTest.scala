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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.neo4j.cypher.internal.aux.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.intValue

class UnionIteratorTest extends CypherFunSuite {
  val state = QueryStateHelper.empty

  test("empty_plus_empty_is_empty") {
    //GIVEN
    val union = createUnion(Iterator.empty, Iterator.empty)

    //THEN
    union shouldBe empty
  }

  test("single_element") {
    // GIVEN
    val singleMap = Map("x" -> 1)
    val union = createUnion(Iterator(singleMap), Iterator.empty)

    //THEN
    union.toList should equal(List( Map("x" -> intValue(1))))
  }

  test("two_elements") {
    //GIVEN
    val aMap = Map("x" -> 1)
    val bMap = Map("x" -> 2)
    val union = createUnion(Iterator(aMap), Iterator(bMap))

    //THEN
    union.toList should equal(List(Map("x" -> intValue(1)), Map("x" -> intValue(2))))
  }

  private def createUnion(aIt: Iterator[Map[String, Any]], bIt: Iterator[Map[String, Any]]): UnionIterator = {
    val a = new FakePipe(aIt)
    val b = new FakePipe(bIt)

    new UnionIterator(Seq(a, b), state)
  }
}
