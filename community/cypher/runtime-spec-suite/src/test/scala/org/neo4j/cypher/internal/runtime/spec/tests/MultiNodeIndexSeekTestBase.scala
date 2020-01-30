/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Node

import scala.collection.mutable.ArrayBuffer

abstract class MultiNodeIndexSeekTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should do double index seek") {
    // given
    val size = Math.max(sizeHint, 10)
    index("Label", "prop")
    val nodes = given {
      nodePropertyGraph(size, {
        case i: Int => Map("prop" -> i % 10)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .multiNodeIndexSeekOperator(_.indexSeek("n:Label(prop=7)"),
                                  _.indexSeek("m:Label(prop=3)"))
      .build()

    // then
    val ns = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 7)
    val ms = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 3)
    val expected = for {n <-ns
                        m <- ms} yield Array(n, m)

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m").withRows(expected)
  }

  test("should do triple index seek") {
    // given
    val size = 100
    index("Label", "prop")
    val nodes = given {
      nodePropertyGraph(size, {
        case i: Int => Map("prop" -> i % 10)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m", "o")
      .multiNodeIndexSeekOperator(_.indexSeek("n:Label(prop=7)"),
                                  _.indexSeek("m:Label(prop=3)"),
                                  _.indexSeek("o:Label(prop=5)"))
      .build()

    // then
    val ns = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 7)
    val ms = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 3)
    val os = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 5)
    val expected = for {n <-ns
                        m <- ms
                        o <- os} yield Array(n, m, o)

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m", "o").withRows(expected)
  }

  test("should handle lots of index seeks") {
    // given
    index("Label", "prop")
    given {
      nodePropertyGraph(sizeHint, {
        case i: Int => Map("prop" -> i)
      }, "Label")
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
      .multiNodeIndexSeekOperator(indexSeeks.map(s => (b: LogicalQueryBuilder) => b.indexSeek(s)): _*)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns(columns: _*).withSingleRow(expected: _*)
  }

  test("should handle various seeks") {
    // given
    val size = 100
    index("Label", "prop")
    val nodes = given {
      nodePropertyGraph(size, {
        case i: Int => Map("prop" -> i % 10)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m", "o")
      .multiNodeIndexSeekOperator(_.indexSeek("n:Label(prop IN ???)",  paramExpr = Some(listOfInt(0, 1, 2))),
                                  _.indexSeek("m:Label(prop IN ???)",  paramExpr = Some(listOfInt(5, 6))),
                                  _.indexSeek("o:Label(prop > 8)"))
      .build()

    // then
    val ns = nodes.filter(n => Set(0, 1, 2).contains(n.getProperty("prop").asInstanceOf[Int]))
    val ms = nodes.filter(m => Set(5, 6).contains(m.getProperty("prop").asInstanceOf[Int]))
    val os = nodes.filter(_.getProperty("prop").asInstanceOf[Int] > 8)
    val expected = for {n <-ns
                        m <- ms
                        o <- os} yield Array(n, m, o)

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m", "o").withRows(expected)
  }

  test("should produce no rows if one seek is empty") {
    // given
    val size = 100
    index("Label", "prop")
    given {
      nodePropertyGraph(size, {
        case i: Int => Map("prop" -> i % 10)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m", "o")
      .multiNodeIndexSeekOperator(_.indexSeek("n:Label(prop IN ???)",  paramExpr = Some(listOfInt(0, 1, 2))),
                                  _.indexSeek("m:Label(prop IN ???)",  paramExpr = Some(listOfInt(5, 6))),
                                  _.indexSeek("o:Label(prop > 10)"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m", "o").withNoRows()
  }

  test("should do double index seek on rhs of apply - multiple input rows") {
    // given
    val size = Math.max(sizeHint, 10)
    index("Label", "prop")
    val nodes = given {
      nodePropertyGraph(size, {
        case i: Int => Map("prop" -> i % 10)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .apply()
      .|.multiNodeIndexSeekOperator(_.indexSeek("n:Label(prop=???)", paramExpr = Some(varFor("i"))),
                                    _.indexSeek("m:Label(prop=???)", paramExpr = Some(varFor("i"))))
      .unwind("range(0, 2) AS i")
      .argument()
      .build()

    // then
    val expected = new ArrayBuffer[Array[Node]]
    (0 to 2).foreach { i =>
      val ns = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == i)
      val ms = nodes.filter(_.getProperty("prop").asInstanceOf[Int] == i)
      val cartesian = for {n <- ns
                           m <- ms} yield Array(n, m)
      expected ++= cartesian
    }

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m").withRows(expected)
  }
}
