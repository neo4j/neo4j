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

class StringMatcherTest extends ParsingTestSupport {

  test("should match strings") {
    new StringMatcher("test") should accept("test")
    new StringMatcher("") should accept("")
  }

  test("should not accept wrong strings") {
    new StringMatcher("aaa") shouldNot accept("a")
    new StringMatcher("a") shouldNot accept("aaa")
    new StringMatcher("A") shouldNot accept("a")
  }

  test("should not accept other types") {
    new StringMatcher("0") shouldNot accept(0)
    new StringMatcher("") shouldNot accept(null)
    new StringMatcher("true") shouldNot accept(true)
  }

}
