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
