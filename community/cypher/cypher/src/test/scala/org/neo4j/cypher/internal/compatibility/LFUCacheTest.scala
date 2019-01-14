/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class LFUCacheTest extends CypherFunSuite {

  test("testClear") {
    val cache = new LFUCache[String, String](5)

    cache.put("A","A")
    cache.put("B","B")
    cache.put("C","C")
    cache.put("D","D")
    cache.put("E","E")

    cache.get("A").isEmpty should be (false)
    cache.get("B").isEmpty should be (false)
    cache.get("C").isEmpty should be (false)
    cache.get("D").isEmpty should be (false)
    cache.get("E").isEmpty should be (false)

    cache.clear() should be (5)  // it returns the number of elements in the cache prior to the clearing

    cache.inner.estimatedSize() should be (0)

    cache.get("A").isEmpty should be (true)
    cache.get("B").isEmpty should be (true)
    cache.get("C").isEmpty should be (true)
    cache.get("D").isEmpty should be (true)
    cache.get("E").isEmpty should be (true)
  }
}
