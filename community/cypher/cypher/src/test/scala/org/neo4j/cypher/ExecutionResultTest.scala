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
package org.neo4j.cypher

import java.util.regex.Pattern

import org.junit.Assert._

class ExecutionResultTest extends ExecutionEngineFunSuite {
  test("columnOrderIsPreserved") {
    val columns = List("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine")

    columns.foreach(createNode)

    val q="match zero, one, two, three, four, five, six, seven, eight, nine " +
      "where id(zero) = 0 AND id(one) = 1 AND id(two) = 2 AND id(three) = 3 AND id(four) = 4 AND id(five) = 5 AND id(six) = 6 AND id(seven) = 7 AND id(eight) = 8 AND id(nine) = 9 " +
      "return zero, one, two, three, four, five, six, seven, eight, nine"

    val result = execute(q)

    assert( result.columns === columns )
    val regex = "zero.*one.*two.*three.*four.*five.*six.*seven.*eight.*nine"
    val pattern = Pattern.compile(regex)

    assertTrue( "Columns did not appear in the expected order: \n" + result.dumpToString(), pattern.matcher(result.dumpToString()).find() )
  }

  test("correctLabelStatisticsForCreate") {
    val result = execute("create (n:foo:bar)")
    val stats  = result.queryStatistics()

    assert(stats.labelsAdded === 2)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForAdd") {
    val n      = createNode()
    val result = execute(s"match (n) where id(n) = ${n.getId} set n:foo:bar")
    val stats  = result.queryStatistics()

    assert(stats.labelsAdded === 2)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForRemove") {
    val n      = createNode()
    execute(s"match (n) where id(n) = ${n.getId} set n:foo:bar")
    val result = execute(s"match (n) where id(n) = ${n.getId} remove n:foo:bar")
    val stats  = result.queryStatistics()

    assert(stats.labelsAdded === 0)
    assert(stats.labelsRemoved === 2)
  }

  test("correctLabelStatisticsForAddAndRemove") {
    val n      = createLabeledNode("foo", "bar")
    val result = execute(s"match (n) where id(n) = ${n.getId} set n:baz remove n:foo:bar")
    val stats  = result.queryStatistics()

    assert(stats.labelsAdded === 1)
    assert(stats.labelsRemoved === 2)
  }


  test("correctLabelStatisticsForLabelAddedTwice") {
    val n      = createLabeledNode("foo", "bar")
    val result = execute(s"match (n) where id(n) = ${n.getId} set n:bar:baz")
    val stats  = result.queryStatistics()

    assert(stats.labelsAdded === 1)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForRemovalOfUnsetLabel") {
    val n      = createLabeledNode("foo", "bar")
    val result = execute(s"match (n) where id(n) = ${n.getId} remove n:baz:foo")
    val stats  = result.queryStatistics()

    assert(stats.labelsAdded === 0)
    assert(stats.labelsRemoved === 1)
  }

  test("correctIndexStatisticsForIndexAdded") {
    val result = execute("create index on :Person(name)")
    val stats  = result.queryStatistics()

    assert(stats.indexesAdded === 1)
    assert(stats.indexesRemoved === 0)
  }

  test("correctIndexStatisticsForIndexAddedTwice") {
    execute("create index on :Person(name)")

    val result = execute("create index on :Person(name)")
    val stats  = result.queryStatistics()

    assert(stats.indexesAdded === 0)
    assert(stats.indexesRemoved === 0)
  }

  test("correctConstraintStatisticsForUniquenessConstraintAdded") {
    val result = execute("create constraint on (n:Person) assert n.name is unique")
    val stats  = result.queryStatistics()

    assert(stats.uniqueConstraintsAdded === 1)
    assert(stats.uniqueConstraintsRemoved === 0)
  }

  test("correctConstraintStatisticsForUniquenessConstraintAddedTwice") {
    execute("create constraint on (n:Person) assert n.name is unique")

    val result = execute("create constraint on (n:Person) assert n.name is unique")
    val stats  = result.queryStatistics()

    assert(stats.uniqueConstraintsAdded === 0)
    assert(stats.uniqueConstraintsRemoved === 0)
  }
}
