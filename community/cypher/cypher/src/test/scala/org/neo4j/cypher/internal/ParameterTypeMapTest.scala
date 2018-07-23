/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.values.storable.Values._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class ParameterTypeMapTest extends CypherFunSuite {
  test("maps should be equal on value types") {
    val v1 = stringValue("value").getClass
    val v2 = intValue(1).getClass
    val v3 = longValue(1L).getClass
    val v4 = booleanArray(Array[Boolean](true)).getClass
    val v5 = stringArray("hi").getClass

    val map = new ParameterTypeMap
    map.put("prop1", v1)
    map.put("prop2", v2)
    map.put("prop3", v3)
    map.put("prop4", v4)
    map.put("prop5", v5)

    map should equal(map)

    val other = new ParameterTypeMap()
    other.put("prop1", v1)
    other.put("prop2", v2)
    other.put("prop3", v3)
    other.put("prop4", v4)
    other.put("prop5", v5)

    map should equal(other)
    other should equal(map)
  }

  test ("maps should not be equal on different value types") {
    val map1 = new ParameterTypeMap()
    map1.put("prop1", stringValue("value").getClass)
    map1.put("prop2", intValue(1).getClass)
    map1.put("prop3", longValue(1L).getClass)
    map1.put("prop4", booleanArray(Array[Boolean](true)).getClass)
    map1.put("prop5", stringArray("hi").getClass)

    val map2 = new ParameterTypeMap()

    map1 should not equal map2
    map2 should not equal map1

    // Compare with almost the same maps
    // -> not all keys
    val map3 = new ParameterTypeMap()
    map3.put("prop1", stringValue("value").getClass)
    map3.put("prop2", intValue(1).getClass)
    map3.put("prop4", booleanArray(Array[Boolean](true)).getClass)
    map3.put("prop3", longValue(1L).getClass)

    map1 should not equal map3
    map3 should not equal map1

    // -> some different value types
    val map4 = new ParameterTypeMap()
    map4.put("prop1", stringValue("value").getClass)
    map4.put("prop2", longValue(1L).getClass)
    map4.put("prop4", booleanArray(Array[Boolean](true)).getClass)
    map4.put("prop3", intValue(1).getClass)
    map4.put("prop5", stringArray("hi").getClass)

    map1 should not equal map4
    map4 should not equal map1
  }
}
