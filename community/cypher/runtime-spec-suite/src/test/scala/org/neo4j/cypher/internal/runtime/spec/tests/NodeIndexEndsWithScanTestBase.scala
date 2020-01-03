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

import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.exceptions.CypherTypeException

abstract class NodeIndexEndsWithScanTestBase[CONTEXT <: RuntimeContext](
                                                             edition: Edition[CONTEXT],
                                                             runtime: CypherRuntime[CONTEXT],
                                                             sizeHint: Int
                                                           ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should be case sensitive for ENDS WITH with indexes") {
    given {
      index("Label", "text")
      nodePropertyGraph(sizeHint, {
        case i if i % 2 == 0 => Map("text" -> "CASE")
        case i if i % 2 == 1 => Map("text" -> "case")
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("x.text AS text")
      .nodeIndexOperator("x:Label(text ENDS WITH 'se')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = List.fill(sizeHint / 2)("case")
    runtimeResult should beColumns("text").withRows(singleColumn(expected))
  }

  test("should be case sensitive for ENDS WITH with unique indexes") {
    given {
      uniqueIndex("Label", "text")
      nodePropertyGraph(sizeHint, {
        case i if i % 2 == 0 => Map("text" -> s"$i CASE")
        case i if i % 2 == 1 => Map("text" -> s"$i case")
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("x.text AS text")
      .nodeIndexOperator("x:Label(text ENDS WITH 'se')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (1 to sizeHint by 2).map(i => s"$i case")
    runtimeResult should beColumns("text").withRows(singleColumn(expected))
  }

  test("should handle null input") {
    given {
      index("Label", "text")
      nodePropertyGraph(sizeHint, {
        case i if i % 2 == 0 => Map("text" -> "CASE")
        case i if i % 2 == 1 => Map("text" -> "case")
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("x.text AS text")
      .nodeIndexOperator("x:Label(text ENDS WITH ???)", paramExpr = Some(nullLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("text").withNoRows()
  }

  test("should handle non-text input") {
    given {
      index("Label", "text")
      nodePropertyGraph(sizeHint, {
        case i if i % 2 == 0 => Map("text" -> "CASE")
        case i if i % 2 == 1 => Map("text" -> "case")
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("x.text AS text")
      .nodeIndexOperator("x:Label(text ENDS WITH 1337)")
      .build()


    // then
    a [CypherTypeException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test("should cache properties") {
    val nodes = given {
      index("Label", "text")
      nodePropertyGraph(sizeHint, {
        case i => Map("text" -> i.toString)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "text")
      .projection("cache[x.text] AS text")
      .nodeIndexOperator("x:Label(text ENDS WITH '1')", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect{ case (n, i) if i.toString.endsWith("1") => Array(n, i.toString)}
    runtimeResult should beColumns("x", "text").withRows(expected)
  }

  test("should cache properties with a unique index") {
    val nodes = given {
      uniqueIndex("Label", "text")
      nodePropertyGraph(sizeHint, {
        case i => Map("text" -> i.toString)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "text")
      .projection("cache[x.text] AS text")
      .nodeIndexOperator("x:Label(text ENDS WITH '1')", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect{ case (n, i) if i.toString.endsWith("1") => Array(n, i.toString)}
    runtimeResult should beColumns("x", "text").withRows(expected)
  }
}
