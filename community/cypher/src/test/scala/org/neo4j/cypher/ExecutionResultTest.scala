/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

  @Test def correctLabelStatisticsForCreate() {
    val result = parseAndExecute("create n:foo:bar")
    val stats  = result.queryStatistics()

    assert(stats.addedLabels === 2)
    assert(stats.removedLabels === 0)
  }

  @Test def correctLabelStatisticsForAdd() {
    val n      = createNode()
    val result = parseAndExecute(s"start n=node(${n.getId}) set n:foo:bar")
    val stats  = result.queryStatistics()

    assert(stats.addedLabels === 2)
    assert(stats.removedLabels === 0)
  }

  @Test def correctLabelStatisticsForRemove() {
    val n      = createNode()
    parseAndExecute(s"start n=node(${n.getId}) set n:foo:bar")
    val result = parseAndExecute(s"start n=node(${n.getId}) remove n:foo:bar")
    val stats  = result.queryStatistics()

    assert(stats.addedLabels === 0)
    assert(stats.removedLabels === 2)
  }

  @Test def correctLabelStatisticsForAddAndRemove() {
    val n      = createLabeledNode("foo", "bar")
    val result = parseAndExecute(s"start n=node(${n.getId}) set n:baz remove n:foo:bar")
    val stats  = result.queryStatistics()

    assert(stats.addedLabels === 1)
    assert(stats.removedLabels === 2)
  }


  @Test def correctLabelStatisticsForLabelAddedTwice() {
    val n      = createLabeledNode("foo", "bar")
    val result = parseAndExecute(s"start n=node(${n.getId}) set n:bar:baz")
    val stats  = result.queryStatistics()

    assert(stats.addedLabels === 1)
    assert(stats.removedLabels === 0)
  }

  @Test def correctLabelStatisticsForRemovalOfUnsetLabel() {
    val n      = createLabeledNode("foo", "bar")
    val result = parseAndExecute(s"start n=node(${n.getId}) remove n:baz:foo")
    val stats  = result.queryStatistics()

    assert(stats.addedLabels === 0)
    assert(stats.removedLabels === 1)
  }
}