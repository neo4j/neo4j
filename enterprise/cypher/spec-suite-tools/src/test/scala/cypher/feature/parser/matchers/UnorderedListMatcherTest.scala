/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
