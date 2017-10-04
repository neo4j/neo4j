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

import java.util.Arrays.asList

import cypher.feature.parser.ParsingTestSupport

class NodeMatcherTest extends ParsingTestSupport {

  test("should match an empty nodes") {
    new NodeMatcher(Set.empty[String].asJava, MapMatcher.EMPTY) should accept(node())
  }

  test("should match a node with labels and properties") {
    new NodeMatcher(Set("L1", "L2", "L3").asJava, MapMatcher.EMPTY) should accept(node(Seq("L1", "L2", "L3")))

    val map = new MapMatcher(Map[String, ValueMatcher]("key" -> new StringMatcher("value")).asJava)
    val matcher = new NodeMatcher(Set.empty[String].asJava, map)
    matcher should accept(node(properties = Map("key" -> "value")))
  }

  test("should not match when labels are wrong") {
    new NodeMatcher(Set("L").asJava, MapMatcher.EMPTY) shouldNot accept(node(Seq("L", "L2")))
    new NodeMatcher(Set("L", "L2").asJava, MapMatcher.EMPTY) shouldNot accept(node(Seq("L")))
  }

  test("should not match when properties are wrong") {
    val matcher = new NodeMatcher(Set.empty[String].asJava, new MapMatcher(Map[String, ValueMatcher]("key" -> new ListMatcher(asList(new StringMatcher("")))).asJava))

    matcher shouldNot accept(node(properties = Map("key" -> List("", " ").asJava)))
  }

  test("should not match when properties are wrong 2") {
    val matcher = new NodeMatcher(Set.empty[String].asJava, new MapMatcher(Map[String, ValueMatcher]("key" -> new ListMatcher(asList(new StringMatcher(""), new StringMatcher(" ")))).asJava))

    matcher shouldNot accept(node(properties = Map("key" -> List("").asJava)))
  }

  test("should not match a non-node") {
    new NodeMatcher(Set.empty[String].asJava, MapMatcher.EMPTY) shouldNot accept("a string")
  }
}
