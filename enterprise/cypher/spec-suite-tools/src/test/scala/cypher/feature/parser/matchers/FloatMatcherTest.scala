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
package cypher.feature.parser.matchers

import cypher.feature.parser.ParsingTestSupport

class FloatMatcherTest extends ParsingTestSupport {

  test("should match simple float") {
    new FloatMatcher(1.0) should accept(1.0)
    new FloatMatcher(.1e1) should accept(1.0)
  }

  test("should match infinities") {
    new FloatMatcher(Double.PositiveInfinity) should accept(Double.PositiveInfinity)
    new FloatMatcher(Double.NegativeInfinity) should accept(Double.NegativeInfinity)
  }

  test("should match NaN") {
    new FloatMatcher(Double.NaN) should accept(Double.NaN)
  }

  test("should not match integers") {
    new FloatMatcher(1.0) shouldNot accept(1)
    new FloatMatcher(0.0) shouldNot accept(0)
  }

  test("should not match strings") {
    new FloatMatcher(0.0) shouldNot accept("0.0")
    new FloatMatcher(0.0e1) shouldNot accept("0")
  }

}
