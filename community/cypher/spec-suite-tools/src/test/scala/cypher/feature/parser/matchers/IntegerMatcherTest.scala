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

import cypher.feature.parser.ParsingTestSupport

class IntegerMatcherTest extends ParsingTestSupport {

  test("should match integers") {
    new IntegerMatcher(0) should accept(0L)
    new IntegerMatcher(-1) should accept(-1L)
    new IntegerMatcher(2) should accept(2L)
  }

  test("should only match longs") {
    new IntegerMatcher(0) shouldNot accept(0)
    new IntegerMatcher(0) shouldNot accept(0.asInstanceOf[Short])
  }

  test("should not accept other types") {
    new IntegerMatcher(0) shouldNot accept(null)
    new IntegerMatcher(0) shouldNot accept("0")
    new IntegerMatcher(0) shouldNot accept(0.0)
  }

  test("should match large ints") {
    new IntegerMatcher(Long.MaxValue) should accept(Long.MaxValue)
  }

}
