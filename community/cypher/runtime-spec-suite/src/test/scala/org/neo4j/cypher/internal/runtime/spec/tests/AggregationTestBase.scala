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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big
import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentAsc
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentDesc
import org.neo4j.cypher.internal.expressions.functions.Avg
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Max
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.StdDev
import org.neo4j.cypher.internal.expressions.functions.StdDevP
import org.neo4j.cypher.internal.expressions.functions.Sum
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.AggregationLargeMorselTestBase.withLargeMorsels
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntBoolean
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntByteArray
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntFloat
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntInteger
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntMap
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntString
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater
import org.neo4j.internal.kernel.api.procs.UserAggregator
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction.BasicUserAggregationFunction
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.scalatest.BeforeAndAfterEach

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Random

abstract class AggregationTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should count(*)") {
    givenGraph { nodeGraph(sizeHint, "Honey") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRows(singleRow(sizeHint))
  }

  test("should count(*) with limit") {
    // given
    givenGraph { nodeGraph(sizeHint, "Honey") }
    val limit = sizeHint / 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .limit(limit)
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRows(singleRow(limit))
  }

  test("should count(*) under apply") {
    val nodesPerLabel = 100
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = aNodes.map(_ => Array[Any](limit))

    runtimeResult should beColumns("c").withRows(expected)
  }

  test("should count(*) under apply when all arguments are filtered out") {
    val nodesPerLabel = 100
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expandAll("(a)-->(b)")
      .|.filter("false")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = aNodes.map(_ => Array[Any](0))

    runtimeResult should beColumns("c").withRows(expected)
  }

  test("should count(*) on single grouping column") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}")
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "c")
      .aggregation(Seq("x.name AS name"), Seq("count(*) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("name", "c").withRows(for (i <- 0 until 10) yield {
      Array[Any](s"bob$i", sizeHint / 10)
    })
  }

  test("should count(*) on single grouping column with limit") {
    // given
    val groupSize = 10
    val groupCount = 2
    val input = inputValues((0 until sizeHint).map(i => Array[Any](s"bob${i / groupSize}")): _*)
    val limit = groupSize * groupCount

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("key", "count")
      .aggregation(Seq("name AS key"), Seq("count(*) AS count"))
      .limit(limit)
      .input(variables = Seq("name"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("key", "count").withRows(for (i <- 0 until groupCount) yield {
      Array[Any](s"bob$i", groupSize)
    })
  }

  test("should count(*) on single grouping column under apply") {
    val nodesPerLabel = 100
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "c")
      .apply()
      .|.aggregation(Seq("a AS a"), Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = aNodes.map(a => Array[Any](a, limit))

    runtimeResult should beColumns("a", "c").withRows(expected)
  }

  test("should count(*) on single grouping column under apply when all arguments are filtered out") {
    val nodesPerLabel = 100
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "c")
      .apply()
      .|.aggregation(Seq("a AS a"), Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expandAll("(a)-->(b)")
      .|.filter("false")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("a", "c").withNoRows()
  }

  test("should count(*) on single primitive grouping column") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .aggregation(Seq("x AS x"), Seq("count(*) AS c"))
      .expand("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "c").withRows(nodes.map { node =>
      Array[Any](node, 2)
    })
  }

  test("should count(*) on single grouping column with nulls") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i, "name" -> s"bob${i % 10}")
          case i: Int if i % 2 == 1 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "c")
      .aggregation(Seq("x.name AS name"), Seq("count(*) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("name", "c").withRows((for (i <- 0 until 10 by 2) yield {
      Array[Any](s"bob$i", sizeHint / 10)
    }) :+ Array[Any](null, sizeHint / 2))
  }

  test("should count(*) on single primitive grouping column with nulls") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .aggregation(Seq("x AS x"), Seq("count(*) AS c"))
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for (node <- nodes if node != null) yield Array[Any](node, 2)
    runtimeResult should beColumns("x", "c").withRows(expected)
  }

  test("should count(*) on two grouping columns") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}", "surname" -> s"bobbins${i / 100}")
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "surname", "c")
      .aggregation(Seq("x.name AS name", "x.surname AS surname"), Seq("count(*) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("name", "surname", "c").withRows(for (i <- 0 until 10; j <- 0 until sizeHint / 100)
      yield {
        Array[Any](s"bob$i", s"bobbins$j", 10)
      })
  }

  test("should count(*) on two primitive grouping columns with nulls") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "c")
      .aggregation(Seq("x AS x", "x2 AS x2"), Seq("count(*) AS c"))
      .projection("x AS x2")
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for (node <- nodes if node != null) yield Array[Any](node, 2)
    runtimeResult should beColumns("x", "c").withRows(expected)
  }

  test("should count(*) on three grouping columns") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i, "name" -> s"bob${i % 10}", "surname" -> s"bobbins${i / 100}", "dead" -> i % 2)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "surname", "dead", "c")
      .aggregation(Seq("x.name AS name", "x.surname AS surname", "x.dead AS dead"), Seq("count(*) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("name", "surname", "dead", "c").withRows(for (
      i <- 0 until 10; j <- 0 until sizeHint / 100
    ) yield {
      Array[Any](s"bob$i", s"bobbins$j", i % 2, 10)
    })
  }

  test("should count(n.prop)") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint / 2)
  }

  test("should count(DISTINCT n.prop)") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i % (sizeHint / 8))
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(DISTINCT x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint / 8)
  }

  test("should count(ASC DISTINCT)") {
    assume(!isParallel)

    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i % (sizeHint / 8))
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(DISTINCT num) ASC AS c"))
      .sort("num ASC")
      .projection("x.num AS num")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint / 8)
  }

  test("should count(DESC DISTINCT)") {
    assume(!isParallel)

    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i % (sizeHint / 8))
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(DISTINCT num) DESC AS c"))
      .sort("num DESC")
      .projection("x.num AS num")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint / 8)
  }

  test("should collect(n.prop)") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("collect(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRows(matching {
      // The order of the collected elements in the list can differ
      case Seq(Array(d: ListValue))
        if d.asArray().toSeq.sorted(ANY_VALUE_ORDERING) == (0 until sizeHint by 2).map(Values.intValue) =>
    })
  }

  test("should collect(n) where n is null") {
    val input = inputValues(Array(Array[Any](null)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("collect(x) AS c"))
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("c").withSingleRow(Array.empty)
  }

  test("should collect(n) where n is null with grouping") {
    val input = inputValues(Array(Array[Any](null)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("c").withSingleRow(Array.empty)
  }

  test("should sum(n.prop)") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("sum(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow((0 until sizeHint by 2).sum)
  }

  test("should fail sum() on mixed numbers and durations I") {
    assertFailOnMixedNumberAndDuration("sum(x)")
  }

  test("should fail avg() on mixed numbers and durations I") {
    assertFailOnMixedNumberAndDuration("avg(x)")
  }

  private def assertFailOnMixedNumberAndDuration(aggregatingFunction: String): Unit = {
    // when
    val NUMBER: Array[Any] = Array(1.0)
    val DURATION: Array[Any] = Array(Duration.of(1, ChronoUnit.NANOS))
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq(s"$aggregatingFunction AS c"))
      .input(variables = Seq("x"))
      .build()

    // then I
    intercept[CypherTypeException] {
      val input = inputValues(NUMBER, DURATION)
      execute(logicalQuery, runtime, input).awaitAll()
    }

    // then II
    intercept[CypherTypeException] {
      val input = inputValues(DURATION, NUMBER)
      execute(logicalQuery, runtime, input).awaitAll()
    }

    val batchSize =
      edition.getSetting(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big).map(_.toInt).getOrElse(10)
    val numberBatches = (0 until batchSize * 10).map(_ => NUMBER)
    val durationBatches = (0 until batchSize * 10).map(_ => DURATION)

    // then III
    intercept[CypherTypeException] {
      val batches: Seq[Array[Any]] = numberBatches ++ durationBatches
      val input = batchedInputValues(batchSize, batches: _*)
      execute(logicalQuery, runtime, input).awaitAll()
    }

    // then IV
    intercept[CypherTypeException] {
      val batches: Seq[Array[Any]] = durationBatches ++ numberBatches
      val input = batchedInputValues(batchSize, batches: _*)
      execute(logicalQuery, runtime, input).awaitAll()
    }
  }

  test("should min(n.prop)") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("min(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow((0 until sizeHint by 2).min)
  }

  test("should min(n.prop) with nulls correctly") {
    // given
    val input = batchedInputValues(
      sizeHint / 8,
      Random.shuffle(Seq.fill[Any](sizeHint - 1)(null) :+ 1).map(x => Array[Any](x)): _*
    ).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("min(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("c").withSingleRow(1)
  }

  test("should max(n.prop)") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("max(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow((0 until sizeHint by 2).max)
  }

  test("should max(n.prop) with nulls correctly") {
    // given
    val input = batchedInputValues(
      sizeHint / 8,
      Random.shuffle(Seq.fill[Any](sizeHint - 1)(null) :+ 1).map(x => Array[Any](x)): _*
    ).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("max(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("c").withSingleRow(1)
  }

  test("should avg(n.prop)") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> (i + 1))
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("avg(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRows(matching {
      case Seq(Array(d: DoubleValue)) if tolerantEquals(sizeHint.toDouble / 2, d.value()) =>
    })
  }

  test("should avg(n.prop) with grouping") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> (i + 1), "name" -> s"bob${i % 10}")
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "c")
      .aggregation(Seq("x.name AS name"), Seq("avg(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val theMiddle = sizeHint.toDouble / 2
    val expectedBobCounts = Map(
      "bob0" -> (theMiddle - 4),
      "bob1" -> (theMiddle - 3),
      "bob2" -> (theMiddle - 2),
      "bob3" -> (theMiddle - 1),
      "bob4" -> theMiddle,
      "bob5" -> (theMiddle + 1),
      "bob6" -> (theMiddle + 2),
      "bob7" -> (theMiddle + 3),
      "bob8" -> (theMiddle + 4),
      "bob9" -> (theMiddle + 5)
    )
    runtimeResult should beColumns("name", "c").withRows(matching {
      case rows: Seq[_] if rows.size == expectedBobCounts.size && rows.forall {
          case Array(s: StringValue, d: DoubleValue) => tolerantEquals(expectedBobCounts(s.stringValue()), d.value())
        } =>
    })
  }

  test("should avg(n.prop) with durations") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> Duration.of(i + 1, ChronoUnit.NANOS))
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("avg(x.num) AS c"))
      .allNodeScan("x")
      .build()

    def asMillis(nanos: Double) = nanos / 1000000
    val runtimeResult = execute(logicalQuery, runtime)
    // then
    runtimeResult should beColumns("c").withRows(matching {
      // convert to millis to be less sensitive to rounding errors
      case Seq(Array(d: DurationValue))
        if tolerantEquals(asMillis(sizeHint.toDouble / 2), asMillis(d.get(ChronoUnit.NANOS))) =>
    })
  }

  test("should avg(n.prop) without numerical overflow") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 1000 == 0 => Map("num" -> (Double.MaxValue - 2.0))
          case i: Int if i % 1000 == 1 => Map("num" -> (Double.MaxValue - 1.0))
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("avg(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRows(matching {
      case Seq(Array(d: DoubleValue)) if tolerantEquals(Double.MaxValue - 1.5, d.value()) =>
    })
  }

  test("should return zero for empty input") {
    // given
    val columns = Seq(
      "countStar",
      "count",
      "countD",
      "avg",
      "avgD",
      "collect",
      "collectD",
      "max",
      "maxD",
      "min",
      "minD",
      "sum",
      "sumD",
      "stdev",
      "stdevD",
      "stdevP",
      "stdevPD"
    )

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(columns: _*)
      .aggregation(
        Seq.empty,
        Seq(
          "count(*) AS countStar",
          "count(x.num) AS count",
          "count(DISTINCT x.num) AS countD",
          "avg(x.num) AS avg",
          "avg(DISTINCT x.num) AS avgD",
          "collect(x.num) AS collect",
          "collect(DISTINCT x.num) AS collectD",
          "max(x.num) AS max",
          "max(DISTINCT x.num) AS maxD",
          "min(x.num) AS min",
          "min(DISTINCT x.num) AS minD",
          "sum(x.num) AS sum",
          "sum(DISTINCT x.num) AS sumD",
          "stdev(x.num) AS stdev",
          "stdev(DISTINCT x.num) AS stdevD",
          "stdevP(x.num) AS stdevP",
          "stdevP(DISTINCT x.num) AS stdevPD"
        )
      )
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns(columns: _*)
      .withSingleRow(
        0,
        0,
        0,
        null,
        null,
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        null,
        null,
        null,
        0,
        0,
        0,
        0,
        0,
        0
      )
  }

  test("should return one row for one input row") {
    // given one row
    val input = inputValues(Array(1))
    val columns = Seq(
      "countStar",
      "count",
      "countD",
      "avg",
      "avgD",
      "collect",
      "collectD",
      "max",
      "maxD",
      "min",
      "minD",
      "sum",
      "sumD",
      "stdev",
      "stdevD",
      "stdevP",
      "stdevPD"
    )

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(columns: _*)
      .aggregation(
        Seq("x AS x"),
        Seq(
          "count(*) AS countStar",
          "count(x) AS count",
          "count(DISTINCT x) AS countD",
          "avg(x) AS avg",
          "avg(DISTINCT x) AS avgD",
          "collect(x) AS collect",
          "collect(DISTINCT x) AS collectD",
          "max(x) AS max",
          "max(DISTINCT x) AS maxD",
          "min(x) AS min",
          "min(DISTINCT x) AS minD",
          "sum(x) AS sum",
          "sum(DISTINCT x) AS sumD",
          "stdev(x) AS stdev",
          "stdev(DISTINCT x) AS stdevD",
          "stdevP(x) AS stdevP",
          "stdevP(DISTINCT x) AS stdevPD"
        )
      )
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns(columns: _*)
      .withSingleRow(
        1,
        1,
        1,
        1,
        1,
        Collections.singletonList(1),
        Collections.singletonList(1),
        1,
        1,
        1,
        1,
        1,
        1,
        0,
        0,
        0,
        0
      )
  }

  test("should return one row for one ordered input row") {
    // given one row
    val input = inputValues(Array(1))
    val columns = Seq(
      "countD",
      "countOD",
      "avgD",
      "avgOD",
      "collectD",
      "collectOD",
      "maxD",
      "maxOD",
      "minD",
      "minOD",
      "sumD",
      "sumOD",
      "stdevD",
      "stdevOD",
      "stdevPD",
      "stdevPOD"
    )

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(columns: _*)
      .aggregation(
        Map.empty[String, Expression],
        Map(
          "countD" -> distinctFunction(Count.name, varFor("x")),
          "countOD" -> distinctFunction(Count.name, ArgumentDesc, varFor("x")),
          "avgD" -> distinctFunction(Avg.name, varFor("x")),
          "avgOD" -> distinctFunction(Avg.name, ArgumentAsc, varFor("x")),
          "collectD" -> distinctFunction(Collect.name, varFor("x")),
          "collectOD" -> distinctFunction(Collect.name, ArgumentAsc, varFor("x")),
          "maxD" -> distinctFunction(Max.name, varFor("x")),
          "maxOD" -> distinctFunction(Max.name, ArgumentAsc, varFor("x")),
          "minD" -> distinctFunction(Min.name, varFor("x")),
          "minOD" -> distinctFunction(Min.name, ArgumentAsc, varFor("x")),
          "sumD" -> distinctFunction(Sum.name, varFor("x")),
          "sumOD" -> distinctFunction(Sum.name, ArgumentAsc, varFor("x")),
          "stdevD" -> distinctFunction(StdDev.name, varFor("x")),
          "stdevOD" -> distinctFunction(StdDev.name, ArgumentAsc, varFor("x")),
          "stdevPD" -> distinctFunction(StdDevP.name, varFor("x")),
          "stdevPOD" -> distinctFunction(StdDevP.name, ArgumentAsc, varFor("x"))
        )
      )
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns(columns: _*)
      .withSingleRow(
        1,
        1,
        1,
        1,
        Collections.singletonList(1),
        Collections.singletonList(1),
        1,
        1,
        1,
        1,
        1,
        1,
        0,
        0,
        0,
        0
      )
  }

  test("should aggregate twice in a row") {
    // given
    val nodes = givenGraph { nodeGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("sum")
      .aggregation(Seq.empty, Seq("sum(partialSum) AS sum"))
      .aggregation(Seq("id(x) % 2 AS key"), Seq("sum(id(x)) AS partialSum"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(_.getId).sum

    runtimeResult should beColumns("sum").withSingleRow(expected)
  }

  test("should aggregation on top of apply with expand and limit and aggregation on rhs of apply") {
    // given
    val nodesPerLabel = 10
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("counts")
      .aggregation(Seq.empty, Seq("collect(c) AS counts"))
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expand("(x)-[:R]->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = aNodes.map(_ => limit).toArray

    runtimeResult should beColumns("counts").withSingleRow(expected)
  }

  test("should aggregate with no grouping on top of apply with expand on RHS") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("ys")
      .aggregation(Seq.empty, Seq("count(y) AS ys"))
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("ys").withSingleRow(aNodes.size * bNodes.size)
  }

  test("should aggregate on top of apply with expand on RHS") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "ys")
      .aggregation(Seq("x AS x"), Seq("count(y) AS ys"))
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map(x => Array[Any](x, bNodes.size))
    runtimeResult should beColumns("x", "ys").withRows(expected)
  }

  test("should perform aggregation with multiple nested apply plans") {
    // given
    val nodesPerLabel = 4
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("key", "sum")
      .aggregation(Seq("innerKey %2 AS key"), Seq("sum(innerSum) AS sum"))
      .apply()
      .|.apply()
      .|.|.aggregation(Seq("outerKey AS innerKey"), Seq("sum(id(innerY)) AS innerSum"))
      .|.|.unwind("ys AS innerY")
      .|.|.argument()
      .|.aggregation(Seq("id(y) % 4 AS outerKey"), Seq("collect(y) AS ys"))
      .|.expandAll("(x)--(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    def outerAgg(from: Node): Seq[(Long, Seq[Node])] = {
      (for {
        rel <- from.getRelationships().asScala.toSeq
        to = rel.getOtherNode(from)
      } yield (from, to)).groupBy { case (_, to) => to.getId % 2 }
        .map { case (key, seq) => (key, seq.map(_._2)) }.toSeq
    }

    val expected = (for {
      x <- aNodes
      (outerKey, ys) <- outerAgg(x)
      innerKey = outerKey
      innerSum = ys.map(_.getId).sum
    } yield (innerKey, innerSum)).groupBy { case (innerKey, _) => innerKey % 2 }
      .map { case (key, seq) => Array[Any](key, seq.map(_._2).sum) }

    // then
    runtimeResult should beColumns("key", "sum").withRows(expected)
  }

  test("should count(cache[n.prop]) with nulls") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(cache[x.num]) AS c"))
      .cacheProperties("cache[x.num]")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint / 2)
  }

  test("should count(cache[n.prop]) with nulls and limit") {
    // given
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i)
        },
        "Honey"
      )
    }
    val limit = sizeHint / 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(cache[x.num]) AS c"))
      .limit(limit)
      .cacheProperties("cache[x.num]")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRows(matching {
      // We don't know how many of these rows have null, so the final count produced by the aggregation can be anywhere between 0 and limit
      case Seq(Array(d: IntegralValue)) if d.longValue() >= 0 && d.longValue() <= limit =>
    })
  }

  test("should count(*) on cache[n.prop] grouping column with nulls") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i, "name" -> s"bob${i % 10}")
          case i: Int if i % 2 == 1 => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "c")
      .aggregation(Seq("cache[x.name] AS name"), Seq("count(*) AS c"))
      .cacheProperties("cache[x.name]")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("name", "c").withRows((for (i <- 0 until 10 by 2) yield {
      Array[Any](s"bob$i", sizeHint / 10)
    }) :+ Array[Any](null, sizeHint / 2))
  }

  test("should count(*) on cache[n.prop] grouping column under apply") {
    val nodesPerLabel = 100
    val (aNodes, _) = givenGraph {
      bipartiteGraph(
        nodesPerLabel,
        "A",
        "B",
        "R",
        aProperties = {
          case i: Int => Map("name" -> s"bob$i")
        }
      )
    }
    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "c")
      .apply()
      .|.aggregation(Seq("cache[a.name] AS name"), Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .cacheProperties("cache[a.name]")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = aNodes.map(a => Array[Any](a.getProperty("name"), limit))

    runtimeResult should beColumns("name", "c").withRows(expected)
  }

  for (distinct <- Seq(false, true)) {
    test(s"should handle percentiles with one disc percentile when distinct=$distinct") {
      givenGraph {
        nodePropertyGraph(
          sizeHint,
          {
            case i: Int => Map("num" -> i % 10)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("p1")
        .projection("m.p1 AS p1")
        .aggregation(
          Map.empty[String, Expression],
          Map(
            "m" -> percentiles(varFor("n"), Seq(0.5), Seq("p1"), Seq(true), distinct)
          )
        )
        .projection("x.num as n")
        .allNodeScan("x")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p1").withRows(singleRow(4))
    }

    test(s"should handle percentiles with one cont percentile when distinct=$distinct") {
      givenGraph {
        nodePropertyGraph(
          sizeHint,
          {
            case i: Int => Map("num" -> i % 10)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("p1")
        .projection("m.p1 AS p1")
        .aggregation(
          Map.empty[String, Expression],
          Map(
            "m" -> percentiles(varFor("n"), Seq(0.5), Seq("p1"), Seq(false), distinct)
          )
        )
        .projection("x.num as n")
        .allNodeScan("x")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p1").withRows(singleRow(4.5))
    }

    test(s"should handle percentiles with two disc percentiles when distinct=$distinct") {
      givenGraph {
        nodePropertyGraph(
          sizeHint,
          {
            case i: Int => Map("num" -> i % 10)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("p1", "p2")
        .projection("m.p1 AS p1", "m.p2 AS p2")
        .aggregation(
          Map.empty[String, Expression],
          Map(
            "m" -> percentiles(varFor("n"), Seq(0.5, 0.5), Seq("p1", "p2"), Seq(true, true), distinct)
          )
        )
        .projection("x.num as n")
        .allNodeScan("x")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p1", "p2").withRows(singleRow(4, 4))
    }

    test(s"should handle percentiles with two cont percentiles when distinct=$distinct") {
      givenGraph {
        nodePropertyGraph(
          sizeHint,
          {
            case i: Int => Map("num" -> i % 10)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("p1", "p2")
        .projection("m.p1 AS p1", "m.p2 AS p2")
        .aggregation(
          Map.empty[String, Expression],
          Map(
            "m" -> percentiles(varFor("n"), Seq(0.5, 0.5), Seq("p1", "p2"), Seq(false, false), distinct)
          )
        )
        .projection("x.num as n")
        .allNodeScan("x")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p1", "p2").withRows(singleRow(4.5, 4.5))
    }

    test(s"should handle percentiles with two cont and disc percentiles when distinct=$distinct") {
      givenGraph {
        nodePropertyGraph(
          sizeHint,
          {
            case i: Int => Map("num" -> i % 10)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("p1", "p2")
        .projection("m.p1 AS p1", "m.p2 AS p2")
        .aggregation(
          Map.empty[String, Expression],
          Map(
            "m" -> percentiles(varFor("n"), Seq(0.5, 0.5), Seq("p1", "p2"), Seq(true, false), distinct)
          )
        )
        .projection("x.num as n")
        .allNodeScan("x")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p1", "p2").withRows(singleRow(4, 4.5))
    }

    test(s"should handle percentiles with nulls when distinct=$distinct") {
      givenGraph {
        nodePropertyGraph(
          sizeHint,
          {
            case i: Int if i % 2 == 0 => Map("num" -> i % 10)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("p1", "p2")
        .projection("m.p1 AS p1", "m.p2 AS p2")
        .aggregation(
          Map.empty[String, Expression],
          Map(
            "m" -> percentiles(prop("n", "num"), Seq(0.5, 0.5), Seq("p1", "p2"), Seq(true, false), distinct)
          )
        )
        .allNodeScan("n")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p1", "p2").withRows(singleRow(4, 4))
    }

    test(s"percentiles should return null for empty input when distinct=$distinct") {
      // given no data

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("p1", "p2")
        .projection("m.p1 AS p1", "m.p2 AS p2")
        .aggregation(
          Map.empty[String, Expression],
          Map(
            "m" -> percentiles(prop("n", "num"), Seq(0.5, 0.5), Seq("p1", "p2"), Seq(true, false), distinct)
          )
        )
        .allNodeScan("n")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p1", "p2").withRows(singleRow(null, null))
    }

    test(s"percentiles should return one row for one input row when distinct=$distinct") {
      givenGraph {
        nodePropertyGraph(
          1,
          {
            case i: Int => Map("num" -> 11)
          },
          "Honey"
        )
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("p1", "p2")
        .projection("m.p1 AS p1", "m.p2 AS p2")
        .aggregation(
          Map.empty[String, Expression],
          Map(
            "m" -> percentiles(prop("n", "num"), Seq(0.5, 0.5), Seq("p1", "p2"), Seq(true, false), distinct)
          )
        )
        .allNodeScan("n")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p1", "p2").withRows(singleRow(11, 11))
    }
  }

  test("should handle percentileDisc") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileDisc(x.num, 0.5) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(4))
  }

  test("should handle percentileDisc with ASC argument") {
    assume(!isParallel)

    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileDisc(num,0.5) ASC AS p"))
      .sort("num ASC")
      .projection("x.num AS num")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(4))
  }

  test("should handle percentileDisc with DESC argument") {
    assume(!isParallel)

    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileDisc(num,0.5) DESC AS p"))
      .sort("num DESC")
      .projection("x.num AS num")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(4))
  }

  test("should handle percentileDisc with nulls") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileDisc(x.num, 0.5) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(4))
  }

  test("should handle percentileDisc with distinct") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileDisc(DISTINCT i, 0.5) AS p"))
      .unwind("[1, 1, 1, 2, 3] AS i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(2))
  }

  test("percentileDisc should return null for empty input") {
    // given no data

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileDisc(x.num, 0.5) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(null))
  }

  test("percentileDisc should return one row for one input row") {
    givenGraph {
      nodePropertyGraph(
        1,
        {
          case i: Int => Map("num" -> 11)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileDisc(x.num, 0.5) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(11))
  }

  test("should handle percentileCont") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileCont(x.num, 0.5) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(4.5))
  }

  test("should handle percentileCont with ASC argument") {
    assume(!isParallel)

    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileCont(num,0.5) ASC AS p"))
      .sort("num ASC")
      .projection("x.num AS num")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(4.5))
  }

  test("should handle percentileCont with DESC argument") {
    assume(!isParallel)

    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileCont(num,0.5) DESC AS p"))
      .sort("num DESC")
      .projection("x.num AS num")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(4.5))
  }

  test("should handle percentileCont with nulls") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileCont(x.num, 0.5) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(4))
  }

  test("should handle percentileCont with distinct") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int if i % 2 == 0 => Map("num" -> i % 10)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileCont(DISTINCT i, 0.5) AS p"))
      .unwind("[1, 1, 1, 2, 3] AS i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(2))
  }

  test("percentileCont should return null for empty input") {
    // given no data

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileCont(x.num, 0.5) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(null))
  }

  test("percentileCont should return one row for one input row") {
    givenGraph {
      nodePropertyGraph(
        1,
        {
          case i: Int => Map("num" -> 11)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("percentileCont(x.num, 0.5) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow(11))
  }

  test("aggregation on rhs should handle lhs rows with extra slots") {

    val query = new LogicalQueryBuilder(this)
      .produceResults("c", "item", "1")
      .projection("1 AS `1`")
      .letSemiApply("item") // RefSlots c, item, 1
      .|.aggregation(Seq.empty, Seq("max(`c`) AS `max(c)`")) // RefSlots c, max(c)
      .|.argument("c") // RefSlots c
      .unwind("[1,2,3] as c") // RefSlots c, item, 1
      .argument()
      .build()

    val runtimeResult = execute(query, runtime)

    runtimeResult should beColumns("c", "item", "1").withRows(Seq(
      Array[Any](1, true, 1),
      Array[Any](2, true, 1),
      Array[Any](3, true, 1)
    ))
  }

  test("grouping aggregation on rhs should handle lhs rows with extra slots") {

    val query = new LogicalQueryBuilder(this)
      .produceResults("c", "item", "1")
      .projection("1 AS `1`")
      .letSemiApply("item") // RefSlots c, item, 1
      .|.filter("c > 0")
      .|.aggregation(Seq("c as c"), Seq("max(`c`) AS `max(c)`")) // RefSlots c, max(c)
      .|.argument("c") // RefSlots c
      .unwind("[1,2,3] as c") // RefSlots c, item, 1
      .argument()
      .build()

    val runtimeResult = execute(query, runtime)

    runtimeResult should beColumns("c", "item", "1").withRows(Seq(
      Array[Any](1, true, 1),
      Array[Any](2, true, 1),
      Array[Any](3, true, 1)
    ))
  }

  test("primitive grouping aggregation on rhs should handle lhs rows with extra slots") {
    val Seq(n) = givenGraph {
      nodeGraph(1)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("a")
      .letSemiApply("c")
      .|.filter("true")
      .|.aggregation(Seq("b as b"), Seq.empty)
      .|.allNodeScan("b", "a")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(query, runtime)

    runtimeResult should beColumns("a").withRows(Seq(
      Array[Any](n)
    ))
  }

  test("distinct average of Infinity is Infinity") {
    // https://trello.com/c/JQtWpAWn/
    // https://neo4j.slack.com/archives/CBPBWR908/p1705405225696079
    val query = new LogicalQueryBuilder(this)
      .produceResults("b")
      .aggregation(Seq("a AS a"), Seq("avg(DISTINCT Infinity) AS b"))
      .unwind("[1, 1, 1, 1, 1, 1] as a")
      .argument()
      .build()

    execute(query, runtime) should beColumns("b").withSingleRow(Double.PositiveInfinity)
  }
}

trait UserDefinedAggregationSupport[CONTEXT <: RuntimeContext] extends BeforeAndAfterEach {
  self: AggregationTestBase[CONTEXT] =>

  private val userAggregationFunctions = {
    val noArgumentNonThreadSafe = UserFunctionSignature.functionSignature("test", "foo0NonThreadSafe")
      .out(Neo4jTypes.NTInteger)
      .build()
    val noArgument = UserFunctionSignature.functionSignature("test", "foo0")
      .out(Neo4jTypes.NTInteger)
      .threadSafe
      .build()
    val oneArgument = UserFunctionSignature.functionSignature("test", "foo1")
      .out(Neo4jTypes.NTInteger)
      .in("in", Neo4jTypes.NTInteger)
      .threadSafe
      .build()
    val twoArguments = UserFunctionSignature.functionSignature("test", "foo2")
      .out(Neo4jTypes.NTInteger)
      .in("in1", Neo4jTypes.NTInteger)
      .in("in2", Neo4jTypes.NTInteger)
      .threadSafe
      .build()
    val defaultArgumets = UserFunctionSignature.functionSignature("test", "defaultValues")
      .out(Neo4jTypes.NTString)
      .in("in1", Neo4jTypes.NTInteger, ntInteger(0L))
      .in("in2", Neo4jTypes.NTFloat, ntFloat(Math.PI))
      .in("in3", Neo4jTypes.NTBoolean, ntBoolean(true))
      .in("in4", Neo4jTypes.NTMap, ntMap(java.util.Map.of("default", "yes")))
      .in("in5", Neo4jTypes.NTByteArray, ntByteArray(Array[Byte](1, 2, 3)))
      .in("in6", Neo4jTypes.NTString, ntString("hello"))
      .threadSafe
      .build()
    val countCreateReducerCalls = UserFunctionSignature.functionSignature("test", "countCreateReducerCalls")
      .out(Neo4jTypes.NTInteger)
      .threadSafe
      .build()
    val countNewUpdaterCalls = UserFunctionSignature.functionSignature("test", "countNewUpdaterCalls")
      .out(Neo4jTypes.NTInteger)
      .threadSafe
      .build()

    Seq(
      new BasicUserAggregationFunction(noArgumentNonThreadSafe) {
        override def create(ctx: Context): UserAggregator = new UserAggregator {
          private var count = 0L

          override def update(input: Array[AnyValue]): Unit = {
            count += 1
          }

          override def result(): AnyValue = Values.longValue(count)
        }
      },
      new BasicUserAggregationFunction(noArgument) {
        override def create(ctx: Context): UserAggregator = new UserAggregator {
          private val count = new AtomicLong(0L)

          override def update(input: Array[AnyValue]): Unit = {
            count.addAndGet(1)
          }

          override def result(): AnyValue = Values.longValue(count.get())
        }
      },
      new BasicUserAggregationFunction(oneArgument) {
        override def create(ctx: Context): UserAggregator = new UserAggregator {
          private val count = new AtomicLong(0L)

          override def update(input: Array[AnyValue]): Unit = {
            count.addAndGet(input(0).asInstanceOf[NumberValue].longValue())
          }

          override def result(): AnyValue = Values.longValue(count.get())
        }
      },
      new BasicUserAggregationFunction(twoArguments) {
        override def create(ctx: Context): UserAggregator = new UserAggregator {
          private val count = new AtomicLong(0L)

          override def update(input: Array[AnyValue]): Unit = {
            count.addAndGet(input(0).asInstanceOf[NumberValue].times(input(1).asInstanceOf[NumberValue]).longValue())
          }

          override def result(): AnyValue = Values.longValue(count.get())
        }
      },
      new BasicUserAggregationFunction(defaultArgumets) {
        override def create(ctx: Context): UserAggregator = new UserAggregator {

          override def update(input: Array[AnyValue]): Unit = {
            input should have size 6

          }

          override def result(): AnyValue = Values.stringValue("yes")
        }
      },
      new BasicUserAggregationFunction(countCreateReducerCalls) {
        private val counter = new LongAdder()
        override def create(ctx: Context): UserAggregator = ???
        override def createReducer(ctx: Context): UserAggregationReducer = new UserAggregationReducer {
          counter.increment()
          override def newUpdater(): UserAggregationUpdater = new UserAggregationUpdater {
            override def update(input: Array[AnyValue]): Unit = {}
            override def applyUpdates(): Unit = {}
          }
          override def result(): AnyValue = Values.longValue(counter.sum())
        }
      },
      new BasicUserAggregationFunction(countNewUpdaterCalls) {
        private val counter = new LongAdder()

        override def create(ctx: Context): UserAggregator = ???

        override def createReducer(ctx: Context): UserAggregationReducer = new UserAggregationReducer {

          override def newUpdater(): UserAggregationUpdater = {
            counter.increment()
            new UserAggregationUpdater {
              override def update(input: Array[AnyValue]): Unit = {}

              override def applyUpdates(): Unit = {}
            }
          }

          override def result(): AnyValue = Values.longValue(counter.sum())
        }
      }
    )
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    userAggregationFunctions.foreach(registerUserAggregation)
  }

  test("should support user-defined aggregation") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("test.foo1(x.num) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withSingleRow(sizeHint * (sizeHint - 1) / 2)
  }

  test("should handle user-defined aggregation that is never called") {
    // given no data

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("test.foo1(x.num) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withSingleRow(0)
  }

  test("should support user-defined aggregation with no argument") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("test.foo0() AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withSingleRow(sizeHint)
  }

  test("should support user-defined aggregation with grouping") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> 10, "name" -> ("name" + i % 10))
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "p")
      .aggregation(Seq("x.name AS name"), Seq("test.foo1(x.num) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (0 to 9).map(i => Array[Any](s"name$i", sizeHint))
    runtimeResult should beColumns("name", "p").withRows(expected)
  }

  test("should support user-defined aggregation under apply") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .apply()
      .|.aggregation(Seq.empty, Seq("test.foo1(y.num) AS p"))
      .|.allNodeScan("y", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(i => Array[Any](i)): _*))

    // then
    val expected = (1 to 10).map(_ => Array[Any]((sizeHint * (sizeHint - 1) / 2)))
    runtimeResult should beColumns("p").withRows(expected)
  }

  test("should support combine user-defined aggregation and aggregation") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p", "c")
      .aggregation(Seq.empty, Seq("test.foo1(x.num) AS p", "count(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p", "c").withRows(singleRow(sizeHint * (sizeHint - 1) / 2, sizeHint))
  }

  test("should support user-defined aggregation with multiple inputs") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num1" -> i, "num2" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("test.foo2(x.num1, x.num2) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withRows(singleRow((sizeHint - 1) * sizeHint * (2 * sizeHint - 1) / 6))
  }

  test("should support user-defined aggregation with multiple arguments and grouping") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> 10, "name" -> ("name" + i % 10))
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name", "p")
      .aggregation(Seq("x.name AS name"), Seq("test.foo2(x.num, x.num) AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (0 to 9).map(i => Array[Any](s"name$i", 10 * sizeHint))
    runtimeResult should beColumns("name", "p").withRows(expected)
  }

  test("should support user-defined aggregation with multiple arguments under apply") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .apply()
      .|.aggregation(Seq.empty, Seq("test.foo2(y.num, y.num) AS p"))
      .|.allNodeScan("y", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(i => Array[Any](i)): _*))

    // then
    val expected = (1 to 10).map(_ => Array[Any]((sizeHint - 1) * sizeHint * (2 * sizeHint - 1) / 6))
    runtimeResult should beColumns("p").withRows(expected)
  }

  test("should support combine user-defined aggregation with multiple arguments and aggregation") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p", "c")
      .aggregation(Seq.empty, Seq("test.foo2(x.num, x.num) AS p", "count(x.num) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p", "c").withRows(singleRow(
      (sizeHint - 1) * sizeHint * (2 * sizeHint - 1) / 6,
      sizeHint
    ))
  }

  test("should support user-defined aggregation with multiple inputs with default") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num1" -> i, "num2" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("test.defaultValues() AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withSingleRow("yes")
  }

  test("should only call createReducer once") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("test.countCreateReducerCalls() AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("p").withSingleRow(1)
  }

  test("should not call newUpdater too many times") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("test.countNewUpdaterCalls() AS p"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    // this can vary but this should hopefully be large enough
    val expectedLimit = 2 * Runtime.getRuntime.availableProcessors()
    runtimeResult should beColumns("p").withRows(matching {
      case Seq(Array(p: IntegralValue)) if p.longValue() < expectedLimit =>
    })
  }

  test("should handle non-thread-safe user-defined aggregation with no argument") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .aggregation(Seq.empty, Seq("test.foo0NonThreadSafe() AS p"))
      .allNodeScan("x")
      .build()

    if (isParallel) {
      // Non-thread-safe UDAFs are not allowed in parallel runtime
      // Should fail at compile-time
      intercept[CantCompileQueryException] {
        execute(logicalQuery, runtime)
      }
    } else {
      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("p").withSingleRow(sizeHint)
    }
  }
}

abstract class AggregationLargeMorselTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](withLargeMorsels(edition), runtime) {

  test("collect in sub queries under limit and big morsels") {
    val personSize = 60
    val enemySize = 2
    val enemyOfEnemySize = 2
    val enemyType = RelationshipType.withName("HAS_ENEMY")
    val potentialFriends = givenGraph {
      for {
        person <- nodeGraph(personSize, "Person")
        enemy <- nodeGraph(enemySize, "Prick")
        _ = person.createRelationshipTo(enemy, enemyType)
        enemyOfEnemy <- nodeGraph(enemyOfEnemySize, "Prick")
        _ = enemy.createRelationshipTo(enemyOfEnemy, enemyType)
      } yield {
        enemyOfEnemy.setProperty("name", enemyOfEnemy.getId)
        enemyOfEnemy
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .limit(25)
      .apply()
      .|.aggregation(Seq(), Seq("collect(potentialFriends) AS result"))
      .|.apply()
      .|.|.aggregation(Seq(), Seq("collect(enemyOfEnemy.name) AS potentialFriends"))
      .|.|.expandAll("(enemy)-[:HAS_ENEMY]->(enemyOfEnemy)")
      .|.|.argument("enemy")
      .|.expandAll("(this)-[:HAS_ENEMY]->(enemy)")
      .|.argument("this")
      .nodeByLabelScan("this", "Person", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = potentialFriends
      .map(p => ValueUtils.asAnyValue(p.getId))
      .grouped(enemySize * enemyOfEnemySize)
      .take(25)
      .map(_.grouped(enemyOfEnemySize).map(_.toSet).toSet)
      .toIndexedSeq

    val result = runtimeResult.awaitAll()
      .map { case Array(singleColumn) => singleColumn }
      .map(_.asInstanceOf[ListValue].asScala.toSet)
      .map(_.map(_.asInstanceOf[ListValue].asScala.toSet))

    result shouldBe expected
  }
}

object AggregationLargeMorselTestBase {

  def withLargeMorsels[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT]): Edition[CONTEXT] = {
    edition.copyWith(
      additionalConfigs =
        cypher_pipelined_batch_size_big -> Integer.valueOf(1024),
      cypher_pipelined_batch_size_small -> Integer.valueOf(128)
    )
  }
}
