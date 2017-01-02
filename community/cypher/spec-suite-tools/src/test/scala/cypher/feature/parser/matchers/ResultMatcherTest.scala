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
package cypher.feature.parser.matchers

import java.lang.Long.valueOf
import java.util.Arrays.asList
import java.util.Collections

import cypher.feature.parser.ParsingTestSupport

class ResultMatcherTest extends ParsingTestSupport {

  test("should match an empty result") {
    val matcher = new ResultMatcher(Collections.emptyList())

    matcher should accept(result())
  }

  test("should match a result with one empty row") {
    val matcher = new ResultMatcher(asList(new RowMatcher(Map.empty[String, ValueMatcher].asJava)))

    matcher should accept(result(Map.empty))
  }

  test("should match a result with two rows of results") {
    val row1 = new RowMatcher(Map[String, ValueMatcher]("count" -> new IntegerMatcher(10)).asJava)
    val row2 = new RowMatcher(Map[String, ValueMatcher]("count" -> new IntegerMatcher(100)).asJava)
    val matcher = new ResultMatcher(asList(row1, row2))

    matcher should accept(result(Map("count" -> valueOf(10)), Map("count" -> valueOf(100))))
  }

  test("should not match a result with wrong number of rows") {
    val row1 = new RowMatcher(Map[String, ValueMatcher]("count" -> new IntegerMatcher(10)).asJava)
    val row2 = new RowMatcher(Map[String, ValueMatcher]("count" -> new IntegerMatcher(100)).asJava)
    val matcher = new ResultMatcher(asList(row1, row2))

    matcher shouldNot accept(result(Map("count" -> valueOf(10))))
  }

  test("should not match a result with wrong number of rows 2") {
    val row = new RowMatcher(Map[String, ValueMatcher]("count" -> new IntegerMatcher(10)).asJava)
    val matcher = new ResultMatcher(asList(row))

    matcher shouldNot accept(result(Map("count" -> valueOf(10)), Map("count" -> valueOf(100))))
  }

  test("should consider order properly") {
    val row1 = new RowMatcher(Map[String, ValueMatcher]("count" -> new IntegerMatcher(100)).asJava)
    val row2 = new RowMatcher(Map[String, ValueMatcher]("count" -> new IntegerMatcher(10)).asJava)
    val matcher = new ResultMatcher(asList(row1, row2))

    matcher should accept(result(Map("count" -> valueOf(10)), Map("count" -> valueOf(100))))
    matcher shouldNot acceptOrdered(result(Map("count" -> valueOf(10)), Map("count" -> valueOf(100))))
    matcher should acceptOrdered(result(Map("count" -> valueOf(100)), Map("count" -> valueOf(10))))
  }

}
