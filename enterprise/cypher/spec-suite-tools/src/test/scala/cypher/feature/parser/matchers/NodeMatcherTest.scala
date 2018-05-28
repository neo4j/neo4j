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
