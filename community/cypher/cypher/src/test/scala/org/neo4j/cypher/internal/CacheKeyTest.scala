/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.QueryOptions.CacheKey
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CacheKeyTest extends CypherFunSuite {

  test("All members should be part of render") {
    //given
    val cacheKey = CacheKey("100",
                            "INFO",
                            "SUPER_FAST",
                            "AGGRESSIVE",
                            "WARP_SPEED",
                            "LUDICROUS_SPEED",
                            "NO_CHANCE",
                            "DEBUGGING_IS_WEAK")

    //then
    cacheKey.productIterator.foreach(x => {
      withClue(s"render must contain '$x''") {
        cacheKey.render.contains(x.toString) shouldBe true
      }
    })
  }
}
