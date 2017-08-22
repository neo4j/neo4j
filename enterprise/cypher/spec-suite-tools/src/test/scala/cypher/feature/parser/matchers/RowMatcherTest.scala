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

import java.lang.Boolean.TRUE

import cypher.feature.parser.ParsingTestSupport

class RowMatcherTest extends ParsingTestSupport {

  test("should match an empty row") {
    val matcher = new RowMatcher(Map.empty[String, ValueMatcher].asJava)
    val actual = Map.empty[String, AnyRef]

    matcher should accept(actual.asJava)
  }

  test("should match a row with one column") {
    val matcher = new RowMatcher(Map[String, ValueMatcher]("key" -> new StringMatcher("value")).asJava)
    val actual = Map[String, AnyRef]("key" -> "value")

    matcher should accept(actual.asJava)
  }

  test("should match a row with several columns") {
    val matcher = new RowMatcher(
      Map[String, ValueMatcher]("key" -> new StringMatcher("value"), "key2" -> new BooleanMatcher(true)).asJava)
    val actual = Map[String, AnyRef]("key" -> "value", "key2" -> TRUE)

    matcher should accept(actual.asJava)
  }

  test("should match a row with nodes and relationships") {
    val nodeMatcher = new NodeMatcher(Set.empty[String].asJava, MapMatcher.EMPTY)
    val relPropsMatcher = new MapMatcher(Map[String, ValueMatcher]("visited" -> new BooleanMatcher(true)).asJava)
    val relationshipMatcher = new RelationshipMatcher("T", relPropsMatcher)
    val matcher = new RowMatcher(Map[String, ValueMatcher]("n" -> nodeMatcher, "r" -> relationshipMatcher).asJava)

    val actual = Map[String, AnyRef]("n" -> node(), "r" -> relationship("T", Map("visited" -> TRUE)))

    matcher should accept(actual.asJava)
  }

  test("should not match if different amount of columns") {
    val matcher = new RowMatcher(Map.empty[String, ValueMatcher].asJava)
    val actual = Map[String, AnyRef]("key" -> "value")

    matcher shouldNot accept(actual.asJava)
  }

  test("should not match if different amount of columns 2") {
    val matcher = new RowMatcher(Map[String, ValueMatcher]("key" -> new StringMatcher("")).asJava)
    val actual = Map.empty[String, AnyRef]

    matcher shouldNot accept(actual.asJava)
  }

  test("should not match if values are different") {
    val matcher = new RowMatcher(Map[String, ValueMatcher]("key" -> new StringMatcher("value1")).asJava)
    val actual = Map[String, AnyRef]("key" -> "value2")

    matcher shouldNot accept(actual.asJava)
  }

}
