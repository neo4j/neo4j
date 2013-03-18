/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.commands.SortItem
import org.neo4j.cypher.internal.commands.expressions.{Literal, Identifier}
import org.neo4j.cypher.internal.symbols.IntegerType
import util.Random


class TopPipeTest extends Assertions {
  @Test def top10From5ReturnsAll() {
    val input = createFakePipeWith(5)
    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(10))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(0, 1, 2, 3, 4))
  }

  @Test def top5From10ReturnsAll() {
    val input = createFakePipeWith(10)
    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(5))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(0, 1, 2, 3, 4))
  }

  @Test def reversedTop5From10ReturnsAll() {
    val in = (0 until 100).toSeq.map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in, "a" -> IntegerType())

    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(5))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(0, 1, 2, 3, 4))
  }

  @Test def emptyInputIsNotAProblem() {
    val input = new FakePipe(Iterator(), "a" -> IntegerType())

    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(5))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List())
  }

  private def createFakePipeWith(count: Int): FakePipe = {

    val r = new Random(1337)

    val in = (0 until count).toSeq.map(i => Map("a" -> i)).sortBy( x => r.nextInt(100))
    new FakePipe(in, "a" -> IntegerType())
  }
}