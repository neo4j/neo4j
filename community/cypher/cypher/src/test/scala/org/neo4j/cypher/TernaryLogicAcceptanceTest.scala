/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher

import org.scalatest.Assertions
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(value = classOf[Parameterized])
class TernaryLogicAcceptanceTest(expectedResult: Any, comparison: String) extends ExecutionEngineJUnitSuite {

  @Test def test() {
    assert(expectedResult === executeScalar[Any]("RETURN " + comparison))
  }
}

object TernaryLogicAcceptanceTest {
  @Parameters(name = "{1} => {0}")
  def parameters: java.util.Collection[Array[Any]] = {
    val list = new java.util.ArrayList[Array[Any]]()

    def add(expectedResult: Any, comparison: String) {
      list.add(Array(expectedResult, comparison))
    }

    add(null, "not null")
    add(true, "null IS NULL")

    add(null, "null = null")
    add(null, "null <> null")
    
    add(null, "null and null")
    add(null, "null and true")
    add(null, "true and null")
    add(false, "false and null")
    add(false, "null and false")
    
    add(null, "null or null")
    add(true, "null or true")
    add(true, "true or null")
    add(null, "false or null")
    add(null, "null or false")
    
    add(null, "null xor null")
    add(null, "null xor true")
    add(null, "true xor null")
    add(null, "false xor null")
    add(null, "null xor false")
    
    add(null, "null in [1,2,3]")
    add(null, "null in [1,2,3,null]")
    add(false, "null in []")
    add(true, "1 in [1,2,3, null]")
    add(null, "5 in [1,2,3, null]")

    list
  }

}
