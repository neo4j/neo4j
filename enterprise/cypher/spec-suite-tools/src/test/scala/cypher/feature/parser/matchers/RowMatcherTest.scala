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
