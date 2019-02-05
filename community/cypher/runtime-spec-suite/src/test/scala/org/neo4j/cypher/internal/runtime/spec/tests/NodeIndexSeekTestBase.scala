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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.v4_0.expressions.{ListLiteral, Null, Variable}
import org.neo4j.cypher.internal.v4_0.logical.plans.{GetValue, IndexOrderAscending, IndexOrderDescending, ManyQueryExpression}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

// Supported by all runtimes
abstract class NodeIndexSeekTestBase[CONTEXT <: RuntimeContext](
                                                                 edition: Edition[CONTEXT],
                                                                 runtime: CypherRuntime[CONTEXT],
                                                                 val sizeHint: Int
                                                               ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should exact (single) seek nodes of an index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withRow(expected)
  }

  test("should exact (multiple) seek nodes of an index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR 30)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20), nodes(30))
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }

  test("should exact (multiple, but empty) seek nodes of an index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop)", customQueryExpression = Some(ManyQueryExpression(ListLiteral(Seq.empty)(InputPosition.NONE))))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should exact (multiple, with null) seek nodes of an index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR ???)", paramExpr = Some(Null()(InputPosition.NONE)))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRow(nodes(20))
  }

  test("should exact (multiple, but identical) seek nodes of an index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20))
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }

  test("should exact seek nodes of a unique index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withRow(expected)
  }

  test("should exact (multiple, but identical) seek nodes of a unique index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR 20)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20))
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }

  test("should exact (multiple, with null) seek nodes of a unique index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR ???)", paramExpr = Some(Null()(InputPosition.NONE)))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRow(nodes(20))
  }

}

// Supported by interpreted, slotted, compiled
trait NodeLockingUniqueIndexSeekTestBase[CONTEXT <: RuntimeContext] {
  self: NodeIndexSeekTestBase[CONTEXT] =>

  test("should exact seek nodes of a locking unique index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withRow(expected)
  }

  test("should exact (multiple, but identical) seek nodes of a locking unique index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR 20)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20))
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }



  test("should cache properties in locking unique index") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cached[x.prop] AS prop")
      .nodeIndexOperator("x:Honey(prop = 10)", GetValue)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "prop").withRow(nodes(10), 10)
  }
}

// Supported by interpreted, slotted, morsel
trait NodeIndexSeekRangeAndCompositeTestBase[CONTEXT <: RuntimeContext] {
  self: NodeIndexSeekTestBase[CONTEXT] =>

  test("should seek nodes of an index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1)
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }

  test("should seek nodes of a unique index with a property") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    uniqueIndex("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1)
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }

  test("should support composite index") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
    },"Honey")
    index("Honey", "prop", "prop2")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 10, prop2 = '10')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(10)
    runtimeResult should beColumns("x").withRow(expected)
  }

  test("should support composite index (multiple results)") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 2 == 0 => Map("prop" -> i % 5, "prop2" -> i % 3)
    },"Honey")
    index("Honey", "prop", "prop2")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 0, prop2 = 0)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect { case (n, i) if i % 2 == 0 && i % 5 == 0 && i % 3 == 0 => n }
    runtimeResult should beColumns("x").withSingleValueRows(expected)
  }

  test("should support composite index (multiple values)") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
    },"Honey")
    index("Honey", "prop", "prop2")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 10 OR 20, prop2 = '10' OR '30')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(10)
    runtimeResult should beColumns("x").withRow(expected)
  }

  test("should cache properties") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cached[x.prop] AS prop")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect{ case (n, i) if (i % 10) == 0 && i > sizeHint / 2 => Array(n, i)}
    runtimeResult should beColumns("x", "prop").withRows(expected)
  }

  test("should cache properties in composite index") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
    },"Honey")
    index("Honey", "prop", "prop2")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop", "prop2")
      .projection("cached[x.prop] AS prop", "cached[x.prop2] AS prop2")
      .nodeIndexOperator("x:Honey(prop = 10, prop2 = '10')", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "prop", "prop2").withRow(nodes(10), 10, "10")
  }

  test("should use existing values from arguments when available") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = ???)", GetValue, paramExpr = Some(Variable("value")(InputPosition.NONE)), argumentIds = Set("value"))
      .input("value")
      .build()

    val input = inputValues(Array(20), Array(50))

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withSingleValueRows(Seq(nodes(20), nodes(50)))
  }

  test("should seek nodes of an index with a property in ascending order") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", indexOrder = IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1)
    runtimeResult should beColumns("x").withSingleValueRows(expected).inOrder
  }

  test("should seek nodes of an index with a property in descending order") {
    // given
    nodeGraph(5, "Milk")
    val nodes = nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("prop" -> i)
    },"Honey")
    index("Honey", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", indexOrder = IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1).reverse
    runtimeResult should beColumns("x").withSingleValueRows(expected).inOrder
  }
}
