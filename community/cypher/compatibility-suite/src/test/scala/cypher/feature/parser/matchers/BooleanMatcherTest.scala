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

class BooleanMatcherTest extends ParsingTestSupport {

  test("should match true") {
    new BooleanMatcher(true) should accept(true)
  }

  test("should match false") {
    new BooleanMatcher(false) should accept(false)
  }

  test("should not match other values") {
    new BooleanMatcher(true) shouldNot accept(false)
    new BooleanMatcher(true) shouldNot accept(null)
    new BooleanMatcher(true) shouldNot accept("")

    new BooleanMatcher(false) shouldNot accept(true)
    new BooleanMatcher(false) shouldNot accept(null)
    new BooleanMatcher(false) shouldNot accept("")
  }

}
