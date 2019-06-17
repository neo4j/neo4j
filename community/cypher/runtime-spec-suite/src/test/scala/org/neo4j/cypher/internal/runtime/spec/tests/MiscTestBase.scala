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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.cypher.internal.logical.plans.{Ascending, Descending}
import org.neo4j.cypher.internal.runtime.spec.{Edition, LogicalQueryBuilder, RowsMatcher, RuntimeTestSuite}
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, RelationshipProxyWrappingValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{NodeReference, RelationshipReference}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

abstract class MiscTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                       runtime: CypherRuntime[CONTEXT]) extends RuntimeTestSuite(edition, runtime) {

  test("should handle allNodeScan") {
    // given
    val nodes = nodeGraph(11)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { n <- nodes } yield Array(n)
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should handle allNodeScan and filter") {
    // given
    val nodes = nodeGraph(11)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("id(x) >= 3")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { n <- nodes if n.getId >= 3 } yield Array(n)
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should handle expand + filter") {
    // given
    val size = 1000
    val (_, rels) = circleGraph(size)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .filter(s"id(y) >= ${size / 2}")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        if r.getEndNode.getId >= size /2
        row <- List(Array(r.getStartNode, r.getEndNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand") {
    // given
    val (_, rels) = circleGraph(10000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        row <- List(Array(r.getStartNode, r.getEndNode),
                    Array(r.getEndNode, r.getStartNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should sort") {
    // given
    circleGraph(10000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort(Seq(Descending("y")))
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(sortedDesc("y"))
  }

  test("should reduce twice in a row") {
    // given
    val nodes = nodeGraph(1000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(sortItems = Seq(Ascending("x")))
      .sort(sortItems = Seq(Descending("x")))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumnInOrder(nodes))
  }

  test("should all node scan and sort on rhs of apply") {
    // given
    val nodes = nodeGraph(10)
    val inputRows = inputValues(nodes.map(node => Array[Any](node)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.sort(sortItems = Seq(Descending("x")))
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputRows)

    runtimeResult should beColumns("x").withRows(rowCount(100))
  }

  // TODO  Sort-Apply-Sort-Bug: re-enable
  ignore("should sort on top of apply with all node scan and sort on rhs of apply") {
    // given
    val nodes = nodeGraph(10)
    val inputRows = inputValues(nodes.map(node => Array[Any](node)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(sortItems = Seq(Descending("x")))
      .apply()
      .|.sort(sortItems = Seq(Descending("x")))
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputRows)

    runtimeResult should beColumns("x").withRows(rowCount(100))
  }

  test("should apply-sort") {
    // given
    circleGraph(1000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.sort(Seq(Descending("y")))
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(groupedBy("x").desc("y"))
  }

  test("should apply-apply-sort") {
    // given
    circleGraph(1000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.apply()
      .|.|.sort(Seq(Ascending("z")))
      .|.|.expandAll("(y)--(z)")
      .|.|.argument()
      .|.sort(Seq(Descending("y")))
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(groupedBy("x", "y").asc("z"))
  }

  test("should complete query with error") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("x = 0/0") // will explode!
      .input(variables = Seq("x"))
      .build()

    // when
    import scala.concurrent.ExecutionContext.global
    val futureResult = Future(consume(execute(logicalQuery, runtime, inputValues(Array(1)))))(global)

    // then
    intercept[org.neo4j.cypher.internal.v4_0.util.ArithmeticException] {
      Await.result(futureResult, 10.seconds)
    }
  }

  test("should complete query with error and close cursors") {
    nodePropertyGraph(1000, {
      case i => Map("prop" -> (i - (1000 / 2)))
    })

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filter("100/n.prop = 1") // will explode!
      .allNodeScan("n")
      .build()

    // when
    import scala.concurrent.ExecutionContext.global
    val futureResult = Future(consume(execute(logicalQuery, runtime)))(global)

    // then
    intercept[org.neo4j.cypher.internal.v4_0.util.ArithmeticException] {
      Await.result(futureResult, 30.seconds)
    }
  }

  test("should prepopulate results") {
    // given
    circleGraph(11)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandAll("(x)-[r]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withRows(populated)
  }

  case object populated extends RowsMatcher {
    override def toString: String = "All entities should have been populated"
    override def matches(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean = {
      rows.forall(row => row.forall {
        case _: NodeReference => false
        case n: NodeProxyWrappingNodeValue => n.isPopulated
        case _ : RelationshipReference => false
        case r: RelationshipProxyWrappingValue => r.isPopulated
        case _ => true
      })
    }
  }
}
