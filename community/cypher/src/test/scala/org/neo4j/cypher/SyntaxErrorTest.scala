package org.neo4j.cypher

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.junit.Assert._
import org.junit.{Assert, Test}



class SyntaxErrorTest {
  def expectError(query: String, expectedError: String) {
    val parser = new CypherParser()
    try {
      parser.parse(query)
      fail("Should have produced the error: "+expectedError)
    } catch {
      case x : SyntaxError => Assert.assertEquals(x.getMessage, expectedError)
    }
  }

  @Test def shouldRaiseErrorWhenSortingOnNode() {
    expectError(
      "start s = (1) return s order by s",
      "Cannot ORDER BY on nodes or relationships")
  }

  @Test def shouldRaiseErrorWhenFinishingAListWithAComma() {
    expectError(
      "start s = (1,2,) return s order by s",
      "Cannot ORDER BY on nodes or relationships")
  }


  //TODO: Write test for       start n=(%A%,%B%,%C%,%D%,) return count(n.property?)

}
