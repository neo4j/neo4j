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
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.values.storable.Values.stringValue

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class ValueHashJoinTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime)
    with RandomValuesTestSupport {

  test("should support simple hash join between two identifiers") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("a.prop=b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n))
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle additional data when joining on two identifiers") {
    // given
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i, "otherProp" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("aProp", "bProp")
      .valueHashJoin("a.prop=b.prop")
      .|.projection("b.otherProp AS bProp")
      .|.allNodeScan("b")
      .projection("a.otherProp AS aProp")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (0 until sizeHint).map(n => Array(n, n))
    runtimeResult should beColumns("aProp", "bProp").withRows(expected)
  }

  test("should join on a cached-property") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("cache[a.prop]=cache[b.prop]")
      .|.filter("cache[b.prop] < 10")
      .|.allNodeScan("b")
      .filter("cache[a.prop] < 20")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n)).take(10)
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle multiple columns") {
    // given
    val relTuples = (for (i <- 0 until sizeHint) yield {
      Seq(
        (i, (i + 1) % sizeHint, "R")
      )
    }).reduce(_ ++ _)
    val nodes = givenGraph {
      val nodes = nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
      connect(nodes, relTuples)
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .valueHashJoin("c.prop=f.prop")
      .|.expand("(e)-[r4]->(f)")
      .|.expand("(d)-[r3]->(e)")
      .|.allNodeScan("d")
      .expand("(b)-[r2]->(c)")
      .expand("(a)-[r1]->(b)")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n))
    runtimeResult should beColumns("c").withRows(expected)
  }

  test("should handle cached properties from both lhs and rhs") {
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("ab")
      .apply()
      .|.valueHashJoin("cache[ab.prop] = cache[b.prop]")
      .|.|.filter("cache[ab.prop] = cache[b.prop]")
      .|.|.allNodeScan("b")
      .|.argument()
      .projection("coalesce(a, null) AS ab")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n))
    runtimeResult should beColumns("ab").withRows(expected)
  }

  test("should join with alias on RHS") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "b2")
      .valueHashJoin("a.prop=b.prop")
      .|.projection("b AS b2")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n, n))
    runtimeResult should beColumns("a", "b", "b2").withRows(expected)
  }

  test("should join with alias on LHS") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "a2", "b")
      .valueHashJoin("a.prop=b.prop")
      .|.allNodeScan("b")
      .projection("a AS a2")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n, n))
    runtimeResult should beColumns("a", "a2", "b").withRows(expected)
  }

  test("should join after expand on empty lhs") {
    // given
    givenGraph {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop", n.getId))
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = y.prop")
      .|.expand("(z)--(y)")
      .|.allNodeScan("z")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join on empty rhs") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = y.prop")
      .|.expand("(z)--(y)")
      .|.allNodeScan("z")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join on empty lhs and rhs") {
    // given
    givenGraph {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop", n.getId))
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = y.prop")
      .|.expand("(z)--(y)")
      .|.allNodeScan("z")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join after expand on rhs") {
    // given
    val unfilteredNodes = givenGraph {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop", n.getId))
      nodes
    }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = z.prop")
      .|.expand("(y)--(z)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, otherNode)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join after expand on lhs") {
    // given
    val unfilteredNodes = givenGraph {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop", n.getId))
      nodes
    }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = z.prop")
      .|.allNodeScan("z")
      .expand("(y)--(x)")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(otherNode, node)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join nested") {
    val nodes = givenGraph {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop", n.getId))
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .valueHashJoin("a.prop = b.prop")
      .|.valueHashJoin("a.prop = b.prop")
      .|.|.allNodeScan("b")
      .|.allNodeScan("a")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    runtimeResult should beColumns("a").withRows(nodes.map(Array[Any](_)))
  }

  test("should work under a cartesian product with cache property in join expression") {
    // given
    nodeIndex("A", "row")
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("row" -> i)
        },
        "A"
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .cartesianProduct()
      .|.valueHashJoin("cache[b.row] = c.row")
      .|.|.nodeByLabelScan("c", "A", IndexOrderNone)
      .|.nodeByLabelScan("b", "A", IndexOrderNone)
      .filter("a.row < 1")
      .nodeIndexOperator("a:A(row)", indexOrder = IndexOrderNone, getValue = _ => GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n))
    runtimeResult should beColumns("c").withRows(expected)
  }

  test("should support simple hash join with apply on lhs and rhs") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("a.prop=b.prop")
      .|.apply()
      .|.|.argument("b")
      .|.allNodeScan("b")
      .apply()
      .|.argument("a")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n))
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle argument cancellation") {
    // given
    val lhsLimit = 3
    val rhsLimit = 3

    val prepareInput = for {
      downstreamRangeTo <- Range.inclusive(0, 2)
      lhsRangeTo <- Range.inclusive(0, lhsLimit * 2)
      rhsRangeTo <- Range.inclusive(0, rhsLimit * 2)
    } yield {
      Array(downstreamRangeTo, lhsRangeTo, rhsRangeTo)
    }
    val input = prepareInput ++ prepareInput ++ prepareInput ++ prepareInput

    val downstreamLimit = input.size / 2
    val upstreamLimit1 = (lhsLimit * rhsLimit) / 2
    val upstreamLimit2 = (0.75 * input.size).toInt

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "a", "b", "c")
      .limit(upstreamLimit2)
      .apply()
      .|.limit(upstreamLimit1)
      .|.valueHashJoin("b = c")
      .|.|.limit(rhsLimit)
      .|.|.unwind("range(0, z-1) as c")
      .|.|.argument()
      .|.limit(lhsLimit)
      .|.unwind("range(0, y-1) as b")
      .|.argument()
      .limit(downstreamLimit)
      .unwind("range(0, x-1) as a")
      .input(variables = Seq("x", "y", "z"))
      .build()

    val result = execute(logicalQuery, runtime, inputValues(input.map(_.toArray[Any]): _*))
    result.awaitAll()
  }

  test("should copy random types") {
    val size = random.nextInt(50) + 50
    val props = Range(0, random.nextInt(8)).map(i => s"prop$i")
    def randomProps(): Map[String, Any] = {
      Map("key" -> random.nextInt(4)) ++ props.map(p => p -> randomValues.nextValue().asObject())
    }
    def randomLabels(): Seq[String] = {
      randomAmong(Seq(Seq("LHS"), Seq("RHS"), Seq("LHS", "RHS")))
    }
    // given
    val nodes = givenGraph {
      nodePropertyGraphFunctional(size, i => randomProps(), i => randomLabels())
    }

    // when
    val produce = Seq("a", "b", "a_alias", "b_alias") ++ props.flatMap(p =>
      Seq(s"lhs_$p", s"lhs_${p}_alias", s"rhs_$p", s"rhs_${p}_alias")
    )
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(produce: _*)
      .valueHashJoin("a.key=b.key")
      .|.projection(props.map(p => s"rhs_$p as rhs_${p}_alias"): _*)
      .|.projection(props.map(p => s"b.$p as rhs_$p"): _*)
      .|.projection("b as b_alias")
      .|.nodeByLabelScan("b", "RHS")
      .projection(props.map(p => s"lhs_$p as lhs_${p}_alias"): _*)
      .projection(props.map(p => s"a.$p as lhs_$p"): _*)
      .projection("a as a_alias")
      .nodeByLabelScan("a", "LHS")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val lhsByKey = nodes.filter(_.hasLabel(Label.label("LHS"))).groupBy(_.getProperty("key"))
    val expected = for {
      rhsNode <- nodes.filter(_.hasLabel(Label.label("RHS")))
      lhsNode <- lhsByKey(rhsNode.getProperty("key"))
    } yield {
      val propValues = props.flatMap { p =>
        val rhsProp = rhsNode.getProperty(p)
        val lhsProp = lhsNode.getProperty(p)
        Seq(lhsProp, lhsProp, rhsProp, rhsProp)
      }
      (Seq(lhsNode, rhsNode, lhsNode, rhsNode) ++ propValues).toArray
    }

    runtimeResult should beColumns(produce: _*).withRows(expected)
  }

  test("should discard columns") {
    assume(runtime.name != "interpreted")

    val probe = recordingProbe("lhsKeep", "lhsDiscard", "rhsKeep", "rhsDiscard")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("lhsKeep", "rhsKeep")
      .prober(probe)
      // We discard here but should not remove since we don't put it in an eager buffer
      .projection("0 as hi")
      .valueHashJoin("i = j")
      // Note, discarding from rhs is not implemented
      .|.projection("toString(j + 2) AS rhsKeep", "toString(j + 3) AS rhsDiscard")
      .|.unwind(s"range(0, $sizeHint) AS j")
      .|.argument()
      .projection("lhsKeep AS lhsKeep")
      .projection("toString(i) AS lhsKeep", "toString(i + 1) AS lhsDiscard")
      .unwind(s"range(0,$sizeHint) AS i")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    result should beColumns("lhsKeep", "rhsKeep")
      .withRows(inAnyOrder(Range.inclusive(0, sizeHint).map(i => Array(s"$i", s"${i + 2}"))))

    probe.seenRows.map(_.toSeq).toSeq should contain theSameElementsAs
      Range.inclusive(0, sizeHint)
        .map(i => Seq(stringValue(s"$i"), null, stringValue(s"${i + 2}"), stringValue(s"${i + 3}")))
  }

  test("should join when join key is alias on rhs") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "b2")
      .valueHashJoin("a.prop=b2.prop")
      .|.projection("b as b2")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n, n))
    runtimeResult should beColumns("a", "b", "b2").withRows(expected)
  }

  test("should join when join key is alias on lhs") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "a2")
      .valueHashJoin("a2.prop=b.prop")
      .|.allNodeScan("b")
      .projection("a as a2")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n, n))
    runtimeResult should beColumns("a", "b", "a2").withRows(expected)
  }

  test("should join when join key is alias on both lhs and rhs") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "a2", "b2")
      .valueHashJoin("a2.prop=b2.prop")
      .|.projection("b as b2")
      .|.allNodeScan("b")
      .projection("a as a2")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n, n, n))
    runtimeResult should beColumns("a", "b", "a2", "b2").withRows(expected)
  }

  test("argument projection on the rhs of a value hash join") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .apply()
      .|.valueHashJoin("b = c")
      .|.|.projection("a AS c")
      .|.|.argument("a")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    execute(query, runtime) should beColumns("c")
  }
}
