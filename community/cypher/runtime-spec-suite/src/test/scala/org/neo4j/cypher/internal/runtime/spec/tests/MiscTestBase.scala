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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.Rows
import org.neo4j.cypher.internal.runtime.spec.RowsMatcher
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.TestPath
import org.neo4j.exceptions.ArithmeticException
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.NodeIdReference
import org.neo4j.values.virtual.RelationshipReference

import java.util.Collections

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

abstract class MiscTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT], runtime: CypherRuntime[CONTEXT])
    extends RuntimeTestSuite(edition, runtime) {

  test("should complete query with error") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("x = 0/0") // will explode!
      .input(variables = Seq("x"))
      .build()

    // when
    val futureResult = Future(consume(execute(logicalQuery, runtime, inputValues(Array(1)))))(global)

    // then
    intercept[ArithmeticException] {
      Await.result(futureResult, 10.seconds)
    }
  }

  test("should complete query with error and close cursors") {
    given {
      nodePropertyGraph(
        1000,
        {
          case i => Map("prop" -> (i - (1000 / 2)))
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filter("100/n.prop = 1") // will explode!
      .allNodeScan("n")
      .build()

    // when
    val futureResult = Future(consume(execute(logicalQuery, runtime)))(global)

    // then
    intercept[ArithmeticException] {
      Await.result(futureResult, 30.seconds)
    }
  }

  test("should prepopulate results") {
    given { circleGraph(11) }

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

  test("should handle expand - aggregation - expand ") {
    // given
    val paths = given { chainGraphs(11, "TO", "TO") }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "bs", "d")
      .expand("(c)-[r3*1..2]->(d)") // assuming we do not fuse var-expand
      .expand("(a)-[r2]->(c)")
      .aggregation(Seq("a AS a"), Seq("collect(b) AS bs"))
      .expand("(a)-[r1]->(b)")
      .allNodeScan("a")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path: TestPath <- paths
      } yield {
        Array[Object](path.startNode, Collections.singletonList(path.nodeAt(1)), path.endNode())
      }

    runtimeResult should beColumns("a", "bs", "d").withRows(expected)
  }

  test("should handle expand - aggregation - filter - expand ") {
    // given
    val paths = given { chainGraphs(11, "TO", "TO") }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "bs", "d")
      .expand("(c)-[r3*1..2]->(d)") // assuming we do not fuse var-expand
      .expand("(a)-[r2]->(c)")
      .filter("size(bs) > 0")
      .aggregation(Seq("a AS a"), Seq("collect(b) AS bs"))
      .expand("(a)-[r1]->(b)")
      .allNodeScan("a")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path: TestPath <- paths
      } yield {
        Array[Object](path.startNode, Collections.singletonList(path.nodeAt(1)), path.endNode())
      }

    runtimeResult should beColumns("a", "bs", "d").withRows(expected)
  }

  case object populated extends RowsMatcher {
    override def toString: String = "All entities should have been populated"

    override def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean = {
      rows.forall(row =>
        row.forall {
          case _: NodeIdReference                 => false
          case n: NodeEntityWrappingNodeValue     => n.isPopulated
          case _: RelationshipReference           => false
          case r: RelationshipEntityWrappingValue => r.isPopulated
          case _                                  => true
        }
      )
    }

    override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
  }
}
