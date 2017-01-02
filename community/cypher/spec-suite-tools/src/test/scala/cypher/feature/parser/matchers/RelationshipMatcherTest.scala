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

class RelationshipMatcherTest extends ParsingTestSupport {

  test("should match relationships") {
    new RelationshipMatcher("T", MapMatcher.EMPTY) should accept(relationship("T"))
  }

  test("should match relationships with properties") {
    val matcher = new RelationshipMatcher("T", new MapMatcher(Map[String, ValueMatcher]("key" -> new FloatMatcher(1e10)).asJava))

    matcher should accept(relationship("T", Map("key" -> java.lang.Double.valueOf(1e10))))
  }

  test("should not match other types") {
    new RelationshipMatcher("T", MapMatcher.EMPTY) shouldNot accept(node())
    new RelationshipMatcher("T", MapMatcher.EMPTY) shouldNot accept(null)
    new RelationshipMatcher("T", MapMatcher.EMPTY) shouldNot accept("relationship")
  }

  test("should not match when different type") {
    new RelationshipMatcher("type1", MapMatcher.EMPTY) shouldNot accept(relationship("type2"))
  }

}
