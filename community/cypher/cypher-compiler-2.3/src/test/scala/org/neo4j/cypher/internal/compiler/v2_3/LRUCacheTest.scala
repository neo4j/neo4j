/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LRUCacheTest extends CypherFunSuite {

  test("shouldStoreSingleValue") {
    val cache = new v2_3.LRUCache[String, String](5)
    cache.getOrElseUpdate("hello", "world")

    cache.get("hello") should equal(Some("world"))
  }

  test("shouldLooseTheFirstOne") {
    val cache = new v2_3.LRUCache[String, String](5)
    fillWithOneToFive(cache)

    cache.getOrElseUpdate("6", "6")

    cache.containsKey("1") should equal(false)
  }

  test("shouldLooseTheLeastUsedItem") {
    val cache = new v2_3.LRUCache[String, String](5)
    fillWithOneToFive(cache)

    cache.get("1")
    cache.get("3")
    cache.get("4")
    cache.get("5")

    cache.put("6", "6")

    cache.containsKey("2") should equal(false);
  }

  def fillWithOneToFive(cache: v2_3.LRUCache[String, String]) {
    cache.put("1", "1")
    cache.put("2", "2")
    cache.put("3", "3")
    cache.put("4", "4")
    cache.put("5", "5")
  }
}
