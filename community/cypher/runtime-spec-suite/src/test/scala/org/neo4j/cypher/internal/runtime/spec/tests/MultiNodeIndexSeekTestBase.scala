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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.index.PropertyIndexTestSupport
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.helpers.collection.Iterables

import scala.collection.mutable.ArrayBuffer

/**
 * Testing of multi node index seek.
 *
 * Please note, more test coverage available from classes inheriting MultiNodeIndexSeekCompatibilityTestRewriter.
 */
abstract class MultiNodeIndexSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](runtime = runtime, edition = edition)
    with PropertyIndexTestSupport[CONTEXT]
    with RandomValuesTestSupport {

  test("should do double index seek") {
    // given
    val size = Math.max(sizeHint, 10)
    nodeIndex(IndexType.RANGE, "Label", "prop")
    val nodes = given {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i % 10)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop=7)", indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop=3)", indexType = IndexType.RANGE)
      )
      .build()

    // then
    val ns = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 7)
    val ms = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 3)
    val expected = for {
      n <- ns
      m <- ms
    } yield Array(n, m)

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m").withRows(expected)
  }

  test("should do triple index seek") {
    // given
    val size = 100
    nodeIndex(IndexType.RANGE, "Label", "prop")
    val nodes = given {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i % 10)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m", "o")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop=7)", indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop=3)", indexType = IndexType.RANGE),
        _.nodeIndexSeek("o:Label(prop=5)", indexType = IndexType.RANGE)
      )
      .build()

    // then
    val ns = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 7)
    val ms = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 3)
    val os = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 5)
    val expected = for {
      n <- ns
      m <- ms
      o <- os
    } yield Array(n, m, o)

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m", "o").withRows(expected)
  }

  test("should handle lots of index seeks") {
    // given
    nodeIndex(IndexType.RANGE, "Label", "prop")
    given {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("prop" -> i)
        },
        "Label"
      )
    }

    val nSeeks = 100
    val nodeVars = (1 to nSeeks).map(i => s"n$i")
    val projections = (1 to nSeeks).map(i => s"n$i.prop AS p$i")
    val columns = (1 to nSeeks).map(i => s"p$i")
    val indexSeeks = nodeVars.map(v => s"$v:Label(prop=42)")
    val expected = (1 to nSeeks).map(_ => 42L)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(columns: _*)
      .projection(projections: _*)
      .multiNodeIndexSeekOperator(indexSeeks.map(s =>
        (b: LogicalQueryBuilder) => b.nodeIndexSeek(s, indexType = IndexType.RANGE)
      ): _*)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns(columns: _*).withSingleRow(expected: _*)
  }

  test("should handle various seeks") {
    // given
    val size = 100
    nodeIndex(IndexType.RANGE, "Label", "prop")
    val nodes = given {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i % 10)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m", "o")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop IN ???)", paramExpr = Some(listOfInt(0, 1, 2)), indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop IN ???)", paramExpr = Some(listOfInt(5, 6)), indexType = IndexType.RANGE),
        _.nodeIndexSeek("o:Label(prop > 8)", indexType = IndexType.RANGE)
      )
      .build()

    // then
    val ns = nodes.filter(n => Set(0, 1, 2).contains(n.getProperty("prop").asInstanceOf[Int]))
    val ms = nodes.filter(m => Set(5, 6).contains(m.getProperty("prop").asInstanceOf[Int]))
    val os = nodes.filter(_.getProperty("prop").asInstanceOf[Int] > 8)
    val expected = for {
      n <- ns
      m <- ms
      o <- os
    } yield Array(n, m, o)

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m", "o").withRows(expected)
  }

  test("should produce no rows if one seek is empty") {
    // given
    val size = 100
    nodeIndex(IndexType.RANGE, "Label", "prop")
    given {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i % 10)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m", "o")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop IN ???)", paramExpr = Some(listOfInt(0, 1, 2)), indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop IN ???)", paramExpr = Some(listOfInt(5, 6)), indexType = IndexType.RANGE),
        _.nodeIndexSeek("o:Label(prop > 10)", indexType = IndexType.RANGE)
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m", "o").withNoRows()
  }

  test("should do double index seek on rhs of apply - multiple input rows") {
    // given
    val size = Math.max(sizeHint, 10)
    nodeIndex(IndexType.RANGE, "Label", "prop")
    val nodes = given {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i % 10)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .apply()
      .|.multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop=???)", paramExpr = Some(varFor("i")), indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop=???)", paramExpr = Some(varFor("i")), indexType = IndexType.RANGE)
      )
      .unwind("range(0, 2) AS i")
      .argument()
      .build()

    // then
    val expected = new ArrayBuffer[Array[Node]]
    (0 to 2).foreach { i =>
      val ns = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == i)
      val ms = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == i)
      val cartesian = for {
        n <- ns
        m <- ms
      } yield Array(n, m)
      expected ++= cartesian
    }

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m").withRows(expected)
  }

  test("should handle empty multi node seek") {
    // given
    val size = Math.max(sizeHint, 10)
    nodeIndex(IndexType.RANGE, "Label", "prop")
    given {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i % 10)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop=7)", indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop IN ???)", paramExpr = Some(listOfInt()), indexType = IndexType.RANGE)
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m").withNoRows()
  }

  test("should handle null multi node seek") {
    // given
    val size = Math.max(sizeHint, 10)
    nodeIndex(IndexType.RANGE, "Label", "prop")
    given {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i % 10)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop=7)", indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop IN ???)", paramExpr = Some(nullLiteral), indexType = IndexType.RANGE)
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m").withNoRows()
  }

  test("should work with multiple index types") {
    val nodes = given {
      nodeIndex(IndexType.RANGE, "Label", "prop")
      nodeIndex(IndexType.TEXT, "Label", "prop")

      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 2 == 0 => Map("prop" -> i)
          case i if i % 2 == 1 => Map("prop" -> i.toString)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "s")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("i:Label(prop > 42)", indexType = IndexType.RANGE),
        _.nodeIndexSeek("s:Label(prop STARTS WITH '1')", indexType = IndexType.TEXT)
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = {
      val strs = nodes.filter { n =>
        n.getProperty("prop") match {
          case s: String => s.startsWith("1")
          case _         => false
        }
      }
      val ints = nodes.filter { n =>
        n.getProperty("prop") match {
          case i: Integer => i > 42
          case _          => false
        }
      }

      for { i <- ints; s <- strs } yield Array(i, s)
    }

    runtimeResult should beColumns("i", "s").withRows(inAnyOrder(expected))
  }

  // fusing specific to test fallthrough with mutating operator
  test("should do delete in same pipeline") {
    // given
    val size = Math.max(sizeHint, 10)
    nodeIndex(IndexType.RANGE, "Label", "prop")
    val nodes = given {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i % 10)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("m")
      .deleteNode("n")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop=7)", indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop=3)", indexType = IndexType.RANGE)
      )
      .build(readOnly = false)

    // then
    val ns = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 7)
    val ms = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 3)
    val expected = for {
      n <- ns
      m <- ms
    } yield Array(m)

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("m").withRows(expected).withStatistics(nodesDeleted = size / 10)
    Iterables.count(tx.getAllNodes) shouldBe size * 0.9
  }

  test("should not create too many nodes after a multi node index seek") {
    // NOTE: using sizeHint here can make the tx state unnecessarily big
    val size = 100
    nodeIndex(IndexType.RANGE, "L", "prop")
    val nodes = given {
      nodePropertyGraph(size, { case i => Map("prop" -> i) }, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r")
      .create(createNode("o", "A", "B", "C"))
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek(
          "n:L(prop IN ???)",
          paramExpr = Some(listOf((0 until 10).map(i => literalInt(i)): _*)),
          indexType = IndexType.RANGE
        ),
        _.nodeIndexSeek(
          "m:L(prop IN ???)",
          paramExpr = Some(listOf((10 until 20).map(i => literalInt(i)): _*)),
          indexType = IndexType.RANGE
        )
      )
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val expected = nodes.take(10).flatMap(r => Seq.fill(10 /*m:L seek*/ * 10 /*range() r*/ )(r))
    runtimeResult should beColumns("n")
      .withRows(singleColumn(expected))
      .withStatistics(nodesCreated = size, labelsAdded = 3 * size)
  }
}
