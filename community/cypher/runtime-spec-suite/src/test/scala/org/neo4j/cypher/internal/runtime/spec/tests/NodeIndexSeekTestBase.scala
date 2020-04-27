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
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

// Supported by all runtimes
abstract class NodeIndexSeekTestBase[CONTEXT <: RuntimeContext](
                                                                 edition: Edition[CONTEXT],
                                                                 runtime: CypherRuntime[CONTEXT],
                                                                 val sizeHint: Int
                                                               ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should exact (single) seek nodes of an index with a property") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should exact (single) seek nodes of an index with a property with multiple matches") {
    val numMatches = sizeHint / 5
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i < numMatches => Map("prop" -> "foo")
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 'foo')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(nodes.take(numMatches).map(Array(_)))
  }

  test("exact single seek should handle null") {
    given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = ???)", paramExpr = Some(nullLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should exact (multiple) seek nodes of an index with a property") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR 30)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20), nodes(30))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should handle null in exact multiple seek") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop IN ???)", paramExpr = Some(nullLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20), nodes(30))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should exact (multiple, but empty) seek nodes of an index with a property") {
    given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop)", customQueryExpression = Some(ManyQueryExpression(listOf())))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should exact (multiple, with null) seek nodes of an index with a property") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
       nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR ???)", paramExpr = Some(nullLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(nodes(20))
  }

  test("should exact (multiple, but identical) seek nodes of an index with a property") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should exact seek nodes of a unique index with a property") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should exact (multiple, but identical) seek nodes of a unique index with a property") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR 20)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should exact (multiple, with null) seek nodes of a unique index with a property") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR ???)", paramExpr = Some(nullLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(nodes(20))
  }

}

// Supported by interpreted, slotted, compiled
trait NodeLockingUniqueIndexSeekTestBase[CONTEXT <: RuntimeContext] {
  self: NodeIndexSeekTestBase[CONTEXT] =>

  test("should exact seek nodes of a locking unique index with a property") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should exact (multiple, but identical) seek nodes of a locking unique index with a property") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 20 OR 20)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should cache properties in locking unique index") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cache[x.prop] AS prop")
      .nodeIndexOperator("x:Honey(prop = 10)", GetValue)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "prop").withSingleRow(nodes(10), 10)
  }
}

// Supported by interpreted, slotted, pipelined, parallel
trait NodeIndexSeekRangeAndCompositeTestBase[CONTEXT <: RuntimeContext] {
  self: NodeIndexSeekTestBase[CONTEXT] =>

  test("should seek nodes of an index with a property") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1)
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should handle range seeks: > false") {
    given {
      index("L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", 42)
      tx.createNode(Label.label("L")).setProperty("boolean", "wut!")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:L(boolean > ???)", paramExpr = Some(falseLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(3))
  }

  test("should handle range seeks: >= false") {
    given {
      index("L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", 42)
      tx.createNode(Label.label("L")).setProperty("boolean", "wut!")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:L(boolean >= ???)", paramExpr = Some(falseLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(5))
  }

  test("should handle range seeks: < false") {
    given {
      index("L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", 42)
      tx.createNode(Label.label("L")).setProperty("boolean", "wut!")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:L(boolean < ???)", paramExpr = Some(falseLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle range seeks: <= false") {
    given {
      index("L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", 42)
      tx.createNode(Label.label("L")).setProperty("boolean", "wut!")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:L(boolean <= ???)", paramExpr = Some(falseLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(2))
  }

  test("should handle range seeks: > true") {
    given {
      index("L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", 42)
      tx.createNode(Label.label("L")).setProperty("boolean", "wut!")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:L(boolean > ???)", paramExpr = Some(trueLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle range seeks: >= true") {
    given {
      index("L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", 42)
      tx.createNode(Label.label("L")).setProperty("boolean", "wut!")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:L(boolean >= ???)", paramExpr = Some(trueLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(3))
  }

  test("should handle range seeks: < true") {
    given {
      index("L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", 42)
      tx.createNode(Label.label("L")).setProperty("boolean", "wut!")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:L(boolean < ???)", paramExpr = Some(trueLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(2))
  }

  test("should handle range seeks: <= true") {
    given {
      index("L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:L(boolean <= ???)", paramExpr = Some(trueLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(5))
  }

  test("should seek nodes of a unique index with a property") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1)
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should support composite index") {
    val nodes = given {
      index("Honey", "prop", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 10, prop2 = '10')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(10)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should support composite index (multiple results)") {
    val nodes = given {
      index("Honey", "prop", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 2 == 0 => Map("prop" -> i % 5, "prop2" -> i % 3)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 0, prop2 = 0)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect { case (n, i) if i % 2 == 0 && i % 5 == 0 && i % 3 == 0 => n }
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should support composite index (multiple values)") {
    val nodes = given {
      index("Honey", "prop", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = 10 OR 20, prop2 = '10' OR '30')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(10)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should cache properties") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cache[x.prop] AS prop")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.collect{ case (n, i) if (i % 10) == 0 && i > sizeHint / 2 => Array(n, i)}
    runtimeResult should beColumns("x", "prop").withRows(expected)
  }

  test("should cache properties in composite index") {
    val nodes = given {
      index("Honey", "prop", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop", "prop2")
      .projection("cache[x.prop] AS prop", "cache[x.prop2] AS prop2")
      .nodeIndexOperator("x:Honey(prop = 10, prop2 = '10')", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "prop", "prop2").withSingleRow(nodes(10), 10, "10")
  }

  test("should use existing values from arguments when available") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = ???)", GetValue, paramExpr = Some(varFor("value")), argumentIds = Set("value"))
      .input(variables = Seq("value"))
      .build()

    val input = inputValues(Array(20), Array(50))

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(nodes(20), nodes(50))))
  }

  test("should seek nodes of an index with a property in ascending order") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", indexOrder = IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1)
    runtimeResult should beColumns("x").withRows(singleColumnInOrder(expected))
  }

  test("should seek nodes of an index with a property in descending order") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})", indexOrder = IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1).reverse
    runtimeResult should beColumns("x").withRows(singleColumnInOrder(expected))
  }

  test("should handle order in multiple index seek, ascending") {
    val nodes =
      given {
        index("Honey", "prop")
        nodeGraph(5, "Milk")
        nodePropertyGraph(sizeHint, {
          case i => Map("prop" -> i % 10)
        }, "Honey")
      }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .nodeIndexOperator("x:Honey(prop)",
        customQueryExpression = Some(ManyQueryExpression(listOf(literalInt(7), literalInt(2), literalInt(3)))),
        indexOrder = IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val keys = Set(7, 2, 3)
    val expected = nodes.collect {
      case n if keys(n.getProperty("prop").asInstanceOf[Int]) => n.getProperty("prop").asInstanceOf[Int]
    }.sorted
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  test("should handle order in multiple index seek, descending") {
    val nodes =
      given {
        index("Honey", "prop")
        nodeGraph(5, "Milk")
        nodePropertyGraph(sizeHint, {
          case i => Map("prop" -> i % 10)
        }, "Honey")
      }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .nodeIndexOperator("x:Honey(prop)",
        customQueryExpression = Some(ManyQueryExpression(listOf(literalInt(7), literalInt(2), literalInt(3)))),
        indexOrder = IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val keys = Set(7, 2, 3)
    val expected = nodes.collect {
      case n if keys(n.getProperty("prop").asInstanceOf[Int]) => n.getProperty("prop").asInstanceOf[Int]
    }.sorted(Ordering.Int.reverse)
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  test("should handle multiple index seek with overflowing morsels") {
    // given
    given {
      index("A", "prop")
      val nodes = nodePropertyGraph(sizeHint, {
        case i if i % 2 == 0 => Map("prop" -> 42)
        case _ => Map("prop" -> 1337)
      }, "A")
      nodes.foreach( n => {
        n.createRelationshipTo(tx.createNode(), RelationshipType.withName("R1"))
        n.createRelationshipTo(tx.createNode(), RelationshipType.withName("R2"))
        n.createRelationshipTo(tx.createNode(), RelationshipType.withName("R3"))
        n.createRelationshipTo(tx.createNode(), RelationshipType.withName("R4"))
        n.createRelationshipTo(tx.createNode(), RelationshipType.withName("R5"))
      })
    }

    //when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optional() // The optional is here to avoid fully fusing
      .expandAll("(x)-->(y)")
      .nodeIndexOperator("x:A(prop = 42 OR 1337)")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint * 5))
  }
}

// Buggy in compiled runtime
trait ArrayIndexSupport[CONTEXT <: RuntimeContext] {
  self: NodeIndexSeekTestBase[CONTEXT] =>

  test("should exact (single) seek nodes of an index with an array property") {
    val nodes = given {
      index("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> Array[Int](i))
      }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(prop = ???)", paramExpr = Some(listOf(literalInt(20))))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }
}