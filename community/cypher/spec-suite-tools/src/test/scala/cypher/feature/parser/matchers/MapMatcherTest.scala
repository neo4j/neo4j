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
import cypher.feature.parser.matchers.ValueMatcher.NULL_MATCHER

class MapMatcherTest extends ParsingTestSupport {

  test("should match maps") {
    new MapMatcher(Map[String, ValueMatcher]("key" -> new StringMatcher("value")).asJava) should accept(Map("key" -> "value").asJava)
    new MapMatcher(Map.empty[String, ValueMatcher].asJava) should accept(Map.empty.asJava)
  }

  test("should match nested maps") {
    new MapMatcher(Map[String, ValueMatcher]("key" -> new MapMatcher(Map[String, ValueMatcher]("key" -> new FloatMatcher(0.0)).asJava)).asJava) should accept(Map("key" -> Map("key" -> 0.0).asJava).asJava)
  }

  test("should not match maps of different size") {
    new MapMatcher(Map.empty[String, ValueMatcher].asJava) shouldNot accept(Map("k" -> "").asJava)
    new MapMatcher(Map("k" -> NULL_MATCHER).asJava) shouldNot accept(Map.empty.asJava)
    new MapMatcher(Map("k" -> NULL_MATCHER, "k2" -> new BooleanMatcher(true)).asJava) shouldNot accept(Map("k" -> NULL_MATCHER))
  }

  test("should not accept different maps") {
    new MapMatcher(Map[String, ValueMatcher]("key" -> new StringMatcher("value1")).asJava) shouldNot accept(Map("key" -> "value2").asJava)
  }

}
