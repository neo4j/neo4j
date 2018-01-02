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

import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTNumber
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class OptionalPipeTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]

  test("should return results if it finds them") {
    val source = new FakePipe( Iterator(Map("a" -> 1)), "a" -> CTNumber)
    val state = QueryStateHelper.empty

    val result = OptionalPipe(Set("a"), source)().createResults(state).toList

    result should equal(List(Map("a" -> 1)))
  }

  test("should return nulls if it finds no results") {
    val source = new FakePipe( Iterator.empty, "a" -> CTNumber)
    val state = QueryStateHelper.empty

    val result = OptionalPipe(Set("a"), source)().createResults(state).toList

    result should equal(List(Map("a" -> null)))
  }
}
