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
package org.neo4j.cypher

import org.scalatest.Assertions
import org.junit.Test
import org.junit.Assert._
import java.util.regex.Pattern

class ExecutionResultTest extends ExecutionEngineHelper with Assertions {
  @Test def columnOrderIsPreserved() {
    val columns = List("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten")

    columns.foreach(createNode);

    val q="start one=node(1), two=node(2), three=node(3), four=node(4), five=node(5), six=node(6), seven=node(7), eight=node(8), nine=node(9), ten=node(10) " +
      "return one, two, three, four, five, six, seven, eight, nine, ten"

    val result = parseAndExecute(q)

    assert( result.columns === columns )
    val regex = "one.*two.*three.*four.*five.*six.*seven.*eight.*nine.*ten"
    val pattern = Pattern.compile(regex)

    assertTrue( "Columns did not apperar in the expected order: \n" + result.dumpToString(), pattern.matcher(result.dumpToString()).find() );
  }
}