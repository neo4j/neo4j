/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.SortItem
import commands.expressions.{Literal, Identifier}
import symbols._
import org.scalatest.Assertions
import org.junit.Test
import util.Random

class TopPipeTest extends Assertions {
  @Test def top10From5ReturnsAll() {
    val input = createFakePipeWith(5)
    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(10))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(0, 1, 2, 3, 4))
  }

  @Test def top10From3ReturnsAllDesc() {
    val input = createFakePipeWith(3)
    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = false)), Literal(10))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(2, 1, 0))
  }

  @Test def top5From20ReturnsAll() {
    val input = createFakePipeWith(20)
    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(5))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(0, 1, 2, 3, 4))
  }

  @Test def top3From10ReturnsAllDesc() {
    val input = createFakePipeWith(10)
    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = false)), Literal(3))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(9, 8, 7))
  }

  @Test def reversedTop5From10ReturnsAll() {
    val in = (0 until 100).toSeq.map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(5))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(0, 1, 2, 3, 4))
  }

  @Test def sortDuplicateValuesCorrectly() {
    val in = ((0 until 5).toSeq ++ (0 until 5).toSeq).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = false)), Literal(5))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(4, 4, 3, 3, 2))
  }

  @Test def sortDuplicateValuesCorrectlyForSmallList() {
    val in = List(Map("a" -> 0),Map("a" -> 1),Map("a" -> 1))
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = false)), Literal(2))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(1,1))
  }

  @Test def emptyInputIsNotAProblem() {
    val input = new FakePipe(Iterator.empty, "a" -> CTInteger)

    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(5))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List.empty)
  }

  @Test def nullInputIsNotAProblem() {
    val input = new FakePipe(Seq(Map("a"->10),Map("a"->null)), "a" -> CTInteger)

    val pipe = new TopPipe(input, List(SortItem(Identifier("a"), ascending = true)), Literal(5))
    val result = pipe.createResults(QueryStateHelper.empty).map(ctx => ctx("a")).toList

    assert(result === List(10,null))
  }

  private def createFakePipeWith(count: Int): FakePipe = {

    val r = new Random(1337)

    val in = (0 until count).toSeq.map(i => Map("a" -> i)).sortBy( x => 50 - r.nextInt(100))
    new FakePipe(in, "a" -> CTInteger)
  }
}
