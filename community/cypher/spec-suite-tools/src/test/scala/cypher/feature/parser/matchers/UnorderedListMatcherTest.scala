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

import java.util.Arrays.asList
import java.util.Collections.emptyList

import cypher.feature.parser.ParsingTestSupport
import cypher.feature.parser.matchers.ValueMatcher.NULL_MATCHER

class UnorderedListMatcherTest extends ParsingTestSupport {

  test("should match lists") {
    new UnorderedListMatcher(asList(NULL_MATCHER)) should accept(asList(null))
    new UnorderedListMatcher(asList(new BooleanMatcher(true))) should accept(asList(true))
    new UnorderedListMatcher(emptyList()) should accept(emptyList())
  }

  test("should not match lists of different size") {
    new UnorderedListMatcher(emptyList()) shouldNot accept(asList(""))
    new UnorderedListMatcher(asList(new StringMatcher(""))) shouldNot accept(emptyList())
    new UnorderedListMatcher(asList(new StringMatcher(""), new ListMatcher(emptyList()))) shouldNot accept(asList("", emptyList(), 0))
  }

  test("should match nested lists") {
    new UnorderedListMatcher(asList(new UnorderedListMatcher(asList(NULL_MATCHER)))) should accept(asList(asList(null)))
    new UnorderedListMatcher(asList(new UnorderedListMatcher(asList(new IntegerMatcher(0), new BooleanMatcher(false))), new StringMatcher(""))) should accept(asList(asList(0L, false), ""))
  }

  test("should not match different lists") {
    new UnorderedListMatcher(asList(new IntegerMatcher(-1L))) shouldNot accept(asList(1L))
  }

  test("should match arrays (persisted lists)") {
    new UnorderedListMatcher(asList(new StringMatcher("string"))) should accept(Array("string"))
    new UnorderedListMatcher(emptyList()) should accept(Array())
  }

  test("should not match arrays of different size") {
    new UnorderedListMatcher(emptyList()) shouldNot accept(Array(""))
    new UnorderedListMatcher(asList(new StringMatcher(""))) shouldNot accept(Array())
    new UnorderedListMatcher(asList(new StringMatcher(""), new UnorderedListMatcher(emptyList()))) shouldNot accept(Array("", Array(), 0))
  }

  test("should not match different arrays") {
    new UnorderedListMatcher(asList(new IntegerMatcher(-1L))) shouldNot accept(Array(1L))
  }

  test("should match lists in any order") {
    new UnorderedListMatcher(asList(new IntegerMatcher(1L), new IntegerMatcher(2L))) should accept(asList(1L, 2L))
    new UnorderedListMatcher(asList(new IntegerMatcher(1L), new IntegerMatcher(2L))) should accept(asList(2L, 1L))
    new UnorderedListMatcher(asList(new BooleanMatcher(false), new IntegerMatcher(2L), new StringMatcher("foo"))) should accept(asList("foo", false, 2L))
  }

}
