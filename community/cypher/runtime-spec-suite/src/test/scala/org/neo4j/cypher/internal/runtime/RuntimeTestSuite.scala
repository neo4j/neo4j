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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.{QueryResult, RuntimeResult}
import org.neo4j.graphdb.{GraphDatabaseService, Label, Node}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.values.AnyValue
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.mutable.ArrayBuffer

/**
  * Contains helpers, matchers and graph handling to support runtime acceptance test,
  * meaning tests where the query is
  *
  *  - specified as a logical plan
  *  - executed on a real database
  *  - evaluated by it's results
  */
abstract class RuntimeTestSuite[CONTEXT <: RuntimeContext](runtimeContextCreator: RuntimeContextCreator[CONTEXT],
                                                           graphDatabaseFactory: TestGraphDatabaseFactory)
  extends CypherFunSuite
    with BeforeAndAfterEach
{

  var graphDb: GraphDatabaseService = _
  var runtimeTestSupport: RuntimeTestSupport[CONTEXT] = _

  override def beforeEach(): Unit = {
    graphDb = graphDatabaseFactory.newImpermanentDatabase()
    runtimeTestSupport = new RuntimeTestSupport[CONTEXT](graphDb, runtimeContextCreator)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    graphDb.shutdown()
  }

  // EXECUTE

  def execute(logicalQuery: LogicalQuery, runtime: CypherRuntime[CONTEXT]): RuntimeResult =
    runtimeTestSupport.run(logicalQuery, runtime)

  // GRAPHS

  def nodeGraph(nNodes: Int, labels: String*): Seq[Node] = {
    val tx = graphDb.beginTx()
    try {
      tx.success()
      for (i <- 0 until nNodes) yield {
        graphDb.createNode(labels.map(Label.label):_*)
      }
    } finally tx.close()
  }

  // MATCHERS

  protected def beColumns(columns: String*): RuntimeResultMatcher =
    new RuntimeResultMatcher(columns)

  class RuntimeResultMatcher(expectedColumns: Seq[String]) extends Matcher[RuntimeResult] {

    val expectedRows = new ArrayBuffer[Array[AnyValue]]
    var expectedRowIndex: Int = 0

    def withRow(values: Any*): RuntimeResultMatcher = {
      expectedRows += values.toArray.map(ValueUtils.of)
      this
    }

    def withRows[T](rows: Seq[Array[T]]): RuntimeResultMatcher = {
      expectedRows ++= rows.map(row => row.map(ValueUtils.of))
      this
    }

    def withSingleValueRows(values: Seq[Any]): RuntimeResultMatcher = {
      expectedRows ++= values.map(x => Array(ValueUtils.of(x)))
      this
    }

    def withNoRows(): RuntimeResultMatcher = this

    override def apply(left: RuntimeResult): MatchResult = {
      val columns = left.fieldNames().toSeq
      if (columns != expectedColumns)
        MatchResult(false, s"Expected result columns $expectedColumns, got $columns", "")
      else {
        val rows = new ArrayBuffer[Array[AnyValue]]
        left.accept(new QueryResult.QueryResultVisitor[Exception] {
          override def visit(row: QueryResult.Record): Boolean = {
            rows += row.fields()
            true
          }
        })
        MatchResult(
          expectedRows.size == rows.size,
          s"""Expected rows:
            |
            |$expectedRows
            |
            |but got
            |
            |$rows
          """.stripMargin,
          ""
        )
      }
    }
  }
}
