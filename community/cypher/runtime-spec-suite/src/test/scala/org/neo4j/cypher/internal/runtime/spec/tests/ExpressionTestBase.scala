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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.spec.{Edition, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.graphdb.{Label, Node, Relationship}

abstract class ExpressionTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                             runtime: CypherRuntime[CONTEXT]) extends RuntimeTestSuite(edition, runtime) {
  test("should check if label is set on node") {
    // given
    val size = 100
    given {
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode(Label.label("Label"))
        } else {
          tx.createNode()
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasLabel")
      .projection("x:Label AS hasLabel")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasLabel").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("hasLabel is false on non-existing node") {
    // given
    given { tx.createNode(Label.label("Label")) }
    val node = mock[Node]
    when(node.getId).thenReturn(1337L)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasLabel")
      .projection("x:Label AS hasLabel")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(node)))

    // then
    runtimeResult should beColumns("hasLabel").withRows(singleColumn(Seq(false)))
  }

  test("should handle node property access") {
    // given
    val size = 100
    given {
      nodePropertyGraph(size, {
        case i: Int => Map("prop" -> i)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn(0 until size))
  }

  test("should return null if node property is not there") {
    // given
    val size = 100
    given {
      nodePropertyGraph(size, {
        case i: Int => Map("prop" -> i)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.other AS prop")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn((0 until size).map(_ => null)))
  }

  test("should ignore if trying to get node property from node that isn't there") {
    // given
    given { nodePropertyGraph(1, { case i: Int => Map("prop" -> i)}, "Label") }
    val node = mock[Node]
    when(node.getId).thenReturn(1337L)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .input(nodes = Seq("x"))
      .build()
    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(node)))

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn(Seq(null)))
  }

  test("should handle relationship property access") {
    // given
    val size = 100
    val  rels = given {
      val (_, rels) = circleGraph(size, "L")
      rels.foreach(_.setProperty("prop", 42))
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .input(relationships = Seq("x"))
      .build()
    val input = inputValues(rels.map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn((0 until size).map(_ => 42)))
  }

  test("should return null if relationship property is not there") {
    // given
    val size = 100
    val  rels = given {
      val (_, rels) = circleGraph(size, "L")
      rels.foreach(_.setProperty("prop", 42))
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.other AS prop")
      .input(relationships = Seq("x"))
      .build()

    val input = inputValues(rels.map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn((0 until size).map(_ => null)))
  }

  test("should ignore if trying to get relationship property from relationship that isn't there") {
    // given
    given { nodePropertyGraph(1, { case i: Int => Map("prop" -> i)}, "Label") }
    val relationship = mock[Relationship]
    when(relationship.getId).thenReturn(1337L)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .input(relationships = Seq("x"))
      .build()
    val input = inputValues(Array(relationship))
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn(Seq(null)))
  }
}

// Supported by all runtimes that can deal with changes in the tx-state
trait ExpressionWithTxStateChangesTests[CONTEXT <: RuntimeContext] {
  self: ExpressionTestBase[CONTEXT] =>

  test("hasLabel is false on deleted node") {
    // given
    val node = given { tx.createNode(Label.label("Label")) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasLabel")
      .projection("x:Label AS hasLabel")
      .input(nodes = Seq("x"))
      .build()

    node.delete()
    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(node)))

    // then
    runtimeResult should beColumns("hasLabel").withRows(singleColumn(Seq(false)))
  }

  test("should throw if node was deleted before accessing node property") {
    // given
    val nodes = given {
      nodePropertyGraph(1, {
        case i: Int => Map("prop" -> i)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .input(nodes = Seq("x"))
      .build()
    nodes.head.delete()
    val input = inputValues(nodes.map(n => Array[Any](n)): _*).stream()

    // then
    an [EntityNotFoundException] should be thrownBy consume(execute(logicalQuery, runtime, input))
  }

  test("should throw if relationship was deleted before accessing relationship property") {
    // given
    val  rels = given {
      val (_, rels) = circleGraph(2, "L")
      rels.foreach(_.setProperty("prop", 42))
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .input(relationships = Seq("x"))
      .build()
    rels.head.delete()
    val input = inputValues(rels.map(r => Array[Any](r)): _*).stream()

    // then
    an [EntityNotFoundException] should be thrownBy consume(execute(logicalQuery, runtime, input))
  }
}
