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
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util.Collections

object ProjectionTestBase

abstract class ProjectionTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should introduce new variables and keep old ones") {
    // given
    val input = inputValues((0 until sizeHint).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "j")
      .projection("i * 2 AS j")
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (0 until sizeHint).map(i => Array[Any](i, i * 2))
    runtimeResult should beColumns("i", "j").withRows(expected)
  }

  test("should do desugared map projection") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("map")
      .distinct("map AS map")
      .unwind("range(1,10) AS i")
      .nonFuseable()
      .projection(" n {.*} AS map")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(node => Array[Any](Collections.singletonMap("prop", node.getId)))
    runtimeResult should beColumns("map").withRows(expected)
  }

  test("should not leak property values for a node map projection over a -1 id in a non-nullable slot") {
    // given the property tokens, so the projection resolves to the from-store path
    givenGraph {
      nodePropertyGraph(1, { case _ => Map("p1" -> 1L, "p2" -> 2L, "p3" -> 3L, "p4" -> 4L) })
    }
    val input = inputValues((0 until sizeHint).map(_ => Array[Any](VirtualValues.node(-1L))): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("m")
      .projection("n { .p1, .p2, .p3, .p4 } AS m")
      .input(nodes = Seq("n"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then: no crash, and the projection of a non-existent node yields only NO_VALUE -- either the
    // whole map (the from-store path) or every entry -- never a value belonging to another node.
    runtimeResult.awaitAll().foreach { row =>
      withClue(s"map projection over a -1 node leaked a real property value: ${row(0)}") {
        onlyNoValues(row(0)) shouldBe true
      }
    }
  }

  private def onlyNoValues(value: AnyValue): Boolean = value match {
    case v if v eq Values.NO_VALUE => true
    case m: MapValue               => Seq("p1", "p2", "p3", "p4").forall(key => m.get(key) eq Values.NO_VALUE)
    case _                         => false
  }
}
