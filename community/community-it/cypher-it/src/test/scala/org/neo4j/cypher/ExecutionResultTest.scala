/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.junit.jupiter.api.Assertions.assertTrue
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

import java.util.regex.Pattern

import scala.jdk.CollectionConverters.MapHasAsScala

class ExecutionResultTest extends ExecutionEngineFunSuite {

  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  test("columnOrderIsPreserved") {
    val columns = List("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine")

    columns.foreach(createNode)

    val q = "match (zero), (one), (two), (three), (four), (five), (six), (seven), (eight), (nine) " +
      "where id(zero) = 0 AND id(one) = 1 AND id(two) = 2 AND id(three) = 3 AND id(four) = 4 AND id(five) = 5 AND id(six) = 6 AND id(seven) = 7 AND id(eight) = 8 AND id(nine) = 9 " +
      "return zero, one, two, three, four, five, six, seven, eight, nine"

    assert(execute(q).columns === columns)

    val regex = "zero.*one.*two.*three.*four.*five.*six.*seven.*eight.*nine"
    val pattern = Pattern.compile(regex)

    val stringDump = graph.withTx(tx => tx.execute(q).resultAsString())
    assertTrue(pattern.matcher(stringDump).find(), "Columns did not appear in the expected order: \n" + stringDump)
  }

  test("correctLabelStatisticsForCreate") {
    val result = execute("CREATE (n:foo:bar)")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 2)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForCreateWithIs") {
    val result = execute("CREATE (n IS foo)")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 1)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForCreateWith&Conjunction") {
    val result = execute("CREATE (n:foo&bar)")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 2)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForMerge") {
    val result = execute("MERGE (n:foo:bar)")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 2)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForMergeWithIs") {
    val result = execute("MERGE (n IS foo)")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 1)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForMergeWith&Conjunction") {
    val result = execute("MERGE (n:foo&bar&baz)")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 3)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForSet") {
    val n = createNode()
    val result = execute(s"MATCH (n) WHERE id(n) = ${n.getId} SET n:foo:bar")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 2)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForSetWithIs") {
    val n = createNode()
    val result = execute(s"MATCH (n) WHERE id(n) = ${n.getId} SET n IS foo, n IS bar, n IS baz")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 3)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForRemove") {
    val n = createNode()
    execute(s"MATCH (n) WHERE id(n) = ${n.getId} SET n:foo:bar")
    val result = execute(s"MATCH (n) WHERE id(n) = ${n.getId} REMOVE n:foo:bar")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 0)
    assert(stats.labelsRemoved === 2)
  }

  test("correctLabelStatisticsForRemoveWithIs") {
    val n = createNode()
    execute(s"MATCH (n) WHERE id(n) = ${n.getId} SET n:foo")
    val result = execute(s"MATCH (n) WHERE id(n) = ${n.getId} REMOVE n IS foo")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 0)
    assert(stats.labelsRemoved === 1)
  }

  test("correctLabelStatisticsForSetAndRemove") {
    val n = createLabeledNode("foo", "bar")
    val result = execute(s"MATCH (n) WHERE id(n) = ${n.getId} SET n:baz REMOVE n:foo:bar")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 1)
    assert(stats.labelsRemoved === 2)
  }

  test("correctLabelStatisticsForLabelAddedTwice") {
    val n = createLabeledNode("foo", "bar")
    val result = execute(s"MATCH (n) WHERE id(n) = ${n.getId} SET n:bar:baz")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 1)
    assert(stats.labelsRemoved === 0)
  }

  test("correctLabelStatisticsForRemovalOfUnsetLabel") {
    val n = createLabeledNode("foo", "bar")
    val result = execute(s"MATCH (n) WHERE id(n) = ${n.getId} REMOVE n:baz:foo")
    val stats = result.queryStatistics()

    assert(stats.labelsAdded === 0)
    assert(stats.labelsRemoved === 1)
  }

  test("correctIndexStatisticsForIndexAdded") {
    val result = execute("create index for (n:Person) on (n.name)")
    val stats = result.queryStatistics()

    assert(stats.indexesAdded === 1)
    assert(stats.indexesRemoved === 0)
  }

  test("correctIndexStatisticsForIndexWithNameAdded") {
    val result = execute("create index my_index for (n:Person) on (n.name)")
    val stats = result.queryStatistics()

    assert(stats.indexesAdded === 1)
    assert(stats.indexesRemoved === 0)
  }

  test("correctConstraintStatisticsForUniquenessConstraintAdded") {
    val result = execute("create constraint for (n:Person) require n.name is unique")
    val stats = result.queryStatistics()

    assert(stats.uniqueConstraintsAdded === 1)
    assert(stats.constraintsRemoved === 0)
  }

  test("hasNext should not change resultAsString") {
    graph.withTx(tx => {
      val result = tx.execute("UNWIND [1,2,3] AS x RETURN x")
      result.hasNext
      result.resultAsString() should equal(
        """+---+
          || x |
          |+---+
          || 1 |
          || 2 |
          || 3 |
          |+---+
          |3 rows
          |""".stripMargin
      )
    })
  }

  test("next should change resultAsString") {
    graph.withTx(tx => {
      val result = tx.execute("UNWIND [1,2,3] AS x RETURN x")
      result.next().asScala should equal(Map("x" -> 1))
      result.resultAsString() should equal(
        """+---+
          || x |
          |+---+
          || 2 |
          || 3 |
          |+---+
          |2 rows
          |""".stripMargin
      )
    })
  }
}
