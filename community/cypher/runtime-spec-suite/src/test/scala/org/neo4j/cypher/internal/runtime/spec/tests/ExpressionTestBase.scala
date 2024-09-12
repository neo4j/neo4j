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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasALabelOrType
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.SelectivityTracker
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater
import org.neo4j.internal.kernel.api.procs.UserAggregator
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction.BasicUserAggregationFunction
import org.neo4j.kernel.api.procedure.CallableUserFunction.BasicUserFunction
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.virtual.VirtualValues.list

import java.util.Locale

abstract class ExpressionTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT], runtime: CypherRuntime[CONTEXT])
    extends RuntimeTestSuite(edition, runtime) {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    registerFunction(new BasicUserFunction(UserFunctionSignature.functionSignature(new QualifiedName("runtimeName"))
      .out(Neo4jTypes.NTString).threadSafe().build()) {

      override def apply(ctx: Context, input: Array[AnyValue]): AnyValue = {
        Values.stringValue(ctx.procedureCallContext().cypherRuntimeName())
      }
    })

    registerUserAggregation(
      new BasicUserAggregationFunction(UserFunctionSignature.functionSignature(new QualifiedName(
        "aggregate",
        "runtimeName"
      ))
        .out(Neo4jTypes.NTString).threadSafe.build()) {
        override def createReducer(ctx: Context): UserAggregationReducer =
          new UserAggregationReducer with UserAggregationUpdater {
            override def newUpdater(): UserAggregationUpdater = this

            override def result(): AnyValue = Values.stringValue(ctx.procedureCallContext().cypherRuntimeName())

            override def update(input: Array[AnyValue]): Unit = {}

            override def applyUpdates(): Unit = {}
          }

        override def create(ctx: Context): UserAggregator = ???
      }
    )
  }

  test("hasLabel on top of allNodeScan") {
    // given
    val size = 100
    givenGraph {
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode(label("Label"))
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

  test("hasLabel on top of indexScan") {
    // given
    val size = 100
    givenGraph {
      nodeIndex("Label", "prop")
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode(label("Label"), label("Other")).setProperty("prop", i)
        } else {
          tx.createNode(label("Label")).setProperty("prop", i)
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasLabel")
      .projection("x:Other AS hasLabel")
      .nodeIndexOperator("x:Label(prop)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasLabel").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("hasLabel on top of labelNodeScan") {
    // given
    val size = 100
    givenGraph {
      nodeIndex("Label", "prop")
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode(label("Label"), label("Other")).setProperty("prop", i)
        } else {
          tx.createNode(label("Label")).setProperty("prop", i)
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasLabel")
      .projection("x:Other AS hasLabel")
      .nodeByLabelScan("x", "Label", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasLabel").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("hasLabel is false on non-existing node") {
    // given
    givenGraph {
      tx.createNode(label("Label"))
    }
    val node = mock[Node]
    when(node.getElementId).thenReturn("dummy")
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

  test("hasALabel on top of indexScan") {
    // given
    val size = 100
    givenGraph {
      nodeIndex("Label", "prop")
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode(label("Label"), label("Other")).setProperty("prop", i)
        } else {
          tx.createNode().setProperty("prop", i)
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasALabel")
      .projection("x:% AS hasALabel")
      .nodeIndexOperator("x:Label(prop)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasALabel").withRows(singleColumn((0 until size / 2).map(_ => true)))
  }

  test("hasALabel on top of labelNodeScan") {
    // given
    val size = 100
    givenGraph {
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode(label("Label"), label("Other"))
        } else {
          tx.createNode()
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasALabel")
      .projection("x:% AS hasALabel")
      .nodeByLabelScan("x", "Label", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasALabel").withRows(singleColumn((0 until size / 2).map(_ => true)))
  }

  test("hasALabel on top of allNodesScan") {
    // given
    val size = 100
    givenGraph {
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode(label("Label"), label("Other"))
        } else {
          tx.createNode()
        }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasALabel")
      .projection("x:% AS hasALabel")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasALabel").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("hasALabelOrType on null") {
    // given
    val size = 100
    val unfilteredNodes = givenGraph {
      nodeGraph(size)
    }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasALabelOrType")
      .projection(Map("hasALabelOrType" -> HasALabelOrType(varFor("x"))(pos)))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = nodes.map(node => if (node == null) null else false)
    runtimeResult should beColumns("hasALabelOrType").withRows(singleColumn(expected))
  }

  test("hasALabel on null") {
    // given
    val size = 100
    val unfilteredNodes = givenGraph {
      nodeGraph(size)
    }
    val nodes = select(unfilteredNodes, nullProbability = 0.5)
    val input = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasALabel")
      .projection(Map("hasALabel" -> HasALabel(varFor("x"))(pos)))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = nodes.map(node => if (node == null) null else false)
    runtimeResult should beColumns("hasALabel").withRows(singleColumn(expected))
  }

  test("should handle node property access on top of allNode") {
    // given
    val size = 100
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i)
        },
        "Label"
      )
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

  test("should handle node property access on top of labelScan") {
    // given
    val size = 100
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .nodeByLabelScan("x", "Label", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn(0 until size))
  }

  test("should handle node property access on top of indexScan") {
    // given
    val size = 100
    givenGraph {
      nodeIndex("Label", "prop")
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .nodeIndexOperator("x:Label(prop)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn(0 until size))
  }

  test("should handle hasProperty on top of allNode") {
    // given
    val size = 100
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i if i % 2 == 0 => Map("prop" -> i)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasProp")
      .projection("x.prop IS NOT NULL AS hasProp")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasProp").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("should handle hasProperty on top of labelScan") {
    // given
    val size = 100
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i if i % 2 == 0 => Map("prop" -> i)
        },
        "Label"
      )
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasProp")
      .projection("x.prop IS NOT NULL AS hasProp")
      .nodeByLabelScan("x", "Label", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasProp").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("should handle hasProperty on top of indexScan") {
    // given
    val size = 100
    givenGraph {
      nodeIndex("Label", "other")
      nodePropertyGraph(
        size,
        {
          case i if i % 2 == 0 => Map("prop" -> i, "other" -> i)
          case i               => Map("other" -> i)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasProp")
      .projection("x.prop IS NOT NULL AS hasProp")
      .nodeIndexOperator("x:Label(other)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasProp").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("should handle relationship property exists - fuseable with expand") {
    val size = 100
    givenGraph {
      val (_, rels) = circleGraph(size, "Label")
      var i = 0
      rels.foreach { r =>
        if (i % 2 == 0) {
          r.setProperty("prop", i)
        }
        i += 1
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasProp")
      .projection("r.prop IS NOT NULL AS hasProp")
      .expandAll("(n)-[r]->(m)")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasProp").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("should handle relationship property exists - separate pipeline") {
    val size = 100
    givenGraph {
      val (_, rels) = circleGraph(size, "Label")
      var i = 0
      rels.foreach { r =>
        if (i % 2 == 0) {
          r.setProperty("prop", i)
        }
        i += 1
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasProp")
      .apply()
      .|.projection("r.prop IS NOT NULL AS hasProp")
      .|.argument("r")
      .nonFuseable()
      .expandAll("(n)-[r]->(m)")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasProp").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("should return null if node property is not there") {
    // given
    val size = 100
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i: Int => Map("prop" -> i)
        },
        "Label"
      )
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
    givenGraph {
      nodePropertyGraph(1, { case i: Int => Map("prop" -> i) }, "Label")
    }
    val node = mock[Node]
    when(node.getId).thenReturn(1337L)
    when(node.getElementId).thenReturn("dummy")

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
    val rels = givenGraph {
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
    val rels = givenGraph {
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
    givenGraph {
      nodePropertyGraph(1, { case i: Int => Map("prop" -> i) }, "Label")
    }
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

  test("should read property from correct entity (rel/long slot)") {
    // given
    val size = 100
    val halfSize = size / 2
    val nodes =
      givenGraph {
        nodePropertyGraph(size, { case i => Map("prop" -> i) })
      }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("longNodeProp", "refNodeProp")
      .projection("longNode.prop AS longNodeProp", "refNode.prop AS refNodeProp")
      .input(nodes = Seq("longNode"), variables = Seq("refNode"))
      .build()

    val input = inputColumns(1, halfSize, i => nodes(i), i => nodes(halfSize + i))
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (0 until halfSize).map(i => Array[Any](i, halfSize + i))
    runtimeResult should beColumns("longNodeProp", "refNodeProp").withRows(expected)
  }

  test("should read cached property from correct entity (rel/long slot)") {
    // given
    val size = 100
    val halfSize = size / 2
    val nodes =
      givenGraph {
        nodePropertyGraph(size, { case i => Map("prop" -> i) })
      }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("longNodeProp", "refNodeProp")
      .projection("cache[longNode.prop] AS longNodeProp", "cache[refNode.prop] AS refNodeProp")
      .input(nodes = Seq("longNode"), variables = Seq("refNode"))
      .build()

    val input = inputColumns(1, halfSize, i => nodes(i), i => nodes(halfSize + i))
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (0 until halfSize).map(i => Array[Any](i, halfSize + i))
    runtimeResult should beColumns("longNodeProp", "refNodeProp").withRows(expected)
  }

  test("result of all function should be a boolean") {
    // given
    val size = 100
    val input = for (i <- 0 until size) yield Array[Any](i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("all(a IN [x] WHERE [1,2,3]) AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for (i <- 0 until size) yield Array[Any](i, true)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("result of none function should be a boolean") {
    // given
    val size = 100
    val input = for (i <- 0 until size) yield Array[Any](i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("none(a IN [x] WHERE [1,2,3]) AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for (i <- 0 until size) yield Array[Any](i, false)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("result of any function should be a boolean") {
    // given
    val size = 100
    val input = for (i <- 0 until size) yield Array[Any](i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("any(a IN [x] WHERE [1,2,3]) AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for (i <- 0 until size) yield Array[Any](i, true)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("result of single function should be a boolean") {
    // given
    val size = 100
    val input = for (i <- 0 until size) yield Array[Any](i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("single(a IN [x] WHERE [1,2,3]) AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for (i <- 0 until size) yield Array[Any](i, true)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("AND: should fail if all predicates fail for some input") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection("1/x > 1 AND 1/x > 1 AND 1/x > 1 AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    an[org.neo4j.exceptions.ArithmeticException] should be thrownBy consume(runtimeResult)
  }

  test("AND: should return FALSE if at least one predicate is FALSE") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection("1/x > 1 AND TRUE AND FALSE AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    runtimeResult should beColumns("y").withRows(singleColumn(List(false, false, false)))
  }

  test("AND: should fail if one predicate fails and no other is FALSE") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection("1/x > 1 AND TRUE AND TRUE AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    an[org.neo4j.exceptions.ArithmeticException] should be thrownBy consume(runtimeResult)
  }

  test("Reorderable AND: should fail if all predicates fail for some input") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection(Map("y" -> andsReorderable("1/x > 1", "1/x > 1", "1/x > 1")))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    an[org.neo4j.exceptions.ArithmeticException] should be thrownBy consume(runtimeResult)
  }

  test("Reorderable AND: should return FALSE if at least one predicate is FALSE") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection(Map("y" -> andsReorderable("1/x > 1", "TRUE", "FALSE")))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    runtimeResult should beColumns("y").withRows(singleColumn(List(false, false, false)))
  }

  test("Reorderable AND: should fail if one predicate fails and no other is FALSE") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection(Map("y" -> andsReorderable("1/x > 1", "TRUE", "TRUE")))
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    an[org.neo4j.exceptions.ArithmeticException] should be thrownBy consume(runtimeResult)
  }

  test("should filter with reorderable ANDS") {
    // given
    val size = 10 * SelectivityTracker.MIN_ROWS_BEFORE_SORT.toInt
    val nodes = givenGraph {
      for (i <- 0 until size) yield {
        val n = tx.createNode()
        n.setProperty("prop", i % 100)
        n
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .aggregation(Seq.empty, Seq("count(*) AS result"))
      .filterExpressionOrString(andsReorderable("n.prop > 25", "n.prop < 75"), "n.prop <> 42")
      .allNodeScan("n")
      .build()

    val result = execute(logicalQuery, runtime)

    // then
    val expected = nodes.count { n =>
      val prop = n.getProperty("prop").asInstanceOf[Integer]
      prop > 25 && prop < 75 && prop != 42
    }

    result should beColumns("result").withSingleRow(expected)
  }

  test("OR: should fail if all predicates fail for some input") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection("1/x > 0 OR 1/x > 0 OR 1/x > 0 AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    an[org.neo4j.exceptions.ArithmeticException] should be thrownBy consume(runtimeResult)
  }

  test("OR: should return TRUE if at least one predicate is TRUE") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection("1/x > 0 OR FALSE OR TRUE AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    runtimeResult should beColumns("y").withRows(singleColumn(List(true, true, true)))
  }

  test("OR: should fail if one predicate fails and no other is FALSE") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection("1/x > 0 OR FALSE OR FALSE AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array[Any](1), Array[Any](0), Array[Any](1)))

    an[org.neo4j.exceptions.ArithmeticException] should be thrownBy consume(runtimeResult)
  }

  test("should filter with has any label predicate") {
    // given
    val size = 100
    val nodes = givenGraph {
      Range(0, size).map {
        case i if i % 5 == 0 => runtimeTestSupport.tx.createNode()
        case i if i % 5 == 1 => runtimeTestSupport.tx.createNode(label("A"))
        case i if i % 5 == 2 => runtimeTestSupport.tx.createNode(label("B"))
        case i if i % 5 == 3 => runtimeTestSupport.tx.createNode(label("C"))
        case i if i % 5 == 4 => runtimeTestSupport.tx.createNode(label("A"), label("B"), label("C"))
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filterExpression(hasAnyLabel(varFor("n"), "C", "B"))
      .allNodeScan("n")
      .build()

    val result = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(n => n.hasLabel(label("C")) || n.hasLabel(label("B")))
    result should beColumns("n").withRows(singleColumn(expected))
  }

  test("should filter with has any label predicate and input nodes") {
    // given
    val size = 100
    val nodes = givenGraph {
      Range(0, size).map {
        case i if i % 5 == 0 => runtimeTestSupport.tx.createNode()
        case i if i % 5 == 1 => runtimeTestSupport.tx.createNode(label("A"))
        case i if i % 5 == 2 => runtimeTestSupport.tx.createNode(label("B"))
        case i if i % 5 == 3 => runtimeTestSupport.tx.createNode(label("C"))
        case i if i % 5 == 4 => runtimeTestSupport.tx.createNode(label("A"), label("B"), label("C"))
      }
    }

    val input = inputValues(nodes.map(r => Array[Any](r)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filterExpression(hasAnyLabel(varFor("n"), "C", "B"))
      .input(nodes = Seq("n"))
      .build()

    val result = execute(logicalQuery, runtime, input)

    // then
    val expected = nodes.filter(n => n.hasLabel(label("C")) || n.hasLabel(label("B")))
    result should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle non-existing node with has any label expression") {
    // given
    givenGraph {
      tx.createNode(label("Label"))
    }
    val node = mock[Node]
    when(node.getId).thenReturn(1337L)
    when(node.getElementId).thenReturn("dummy")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filterExpression(hasAnyLabel("n", "Label"))
      .input(nodes = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(node)))

    // then
    runtimeResult should beColumns("n").withNoRows()
  }

  test("should get type of relationship") {
    // given
    val size = 11
    givenGraph {
      chainGraphs(size, "TO")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("t")
      .projection("type(r) AS t")
      .expand("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("t").withRows(singleColumn((1 to size).map(_ => "TO")))
  }

  test("should be able to access what runtime that was used in a UDF") {
    // given an empty db

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("runtime")
      .projection("runtimeName() AS runtime")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("runtime").withSingleRow(runtime.name.toLowerCase(Locale.ROOT))
  }

  test("should be able to access what runtime that was used in a UDAF") {
    // given an empty db

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("runtime")
      .aggregation(Seq.empty, Seq("aggregate.runtimeName() AS runtime"))
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("runtime").withSingleRow(runtime.name.toLowerCase(Locale.ROOT))
  }

  test("should handle valueType function") {
    // given, an empty db
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("type")
      .projection("valueType(someThingWithType) AS type")
      .unwind("[true, \"abc\", 1, 2.0] AS someThingWithType")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("type").withRows(
      singleColumn(
        Seq(
          "BOOLEAN NOT NULL",
          "STRING NOT NULL",
          "INTEGER NOT NULL",
          "FLOAT NOT NULL"
        )
      )
    )
  }

  test("should handle valid cosine vector similarity function calls") {
    // given
    givenGraph {
      val node = tx.createNode(label("Label"))
      node.setProperty("vector", Seq(1.0, 0.0).toArray)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("similarity", "leftNull", "rightNull")
      .projection(
        "vector.similarity.cosine(args[0], args[1]) AS similarity",
        "vector.similarity.cosine(NULL, args[1]) AS leftNull",
        "vector.similarity.cosine(args[0], NULL) AS rightNull"
      )
      .unwind("""[
                |[[1.0, 0.0], [0.0, 1.0]],
                |[[0.0, 1.0], n.vector],
                |[n.vector, [0.0, 1.0]],
                |[n.vector, n.vector],
                |[n.vector, NULL],
                |[NULL, n.vector],
                |[NULL, NULL]
                |] AS args""".stripMargin.replace("\n", ""))
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("similarity", "leftNull", "rightNull").withRows(
      inOrder(
        Seq(
          Array(0.5, NO_VALUE, NO_VALUE),
          Array(0.5, NO_VALUE, NO_VALUE),
          Array(0.5, NO_VALUE, NO_VALUE),
          Array(1.0, NO_VALUE, NO_VALUE),
          Array(NO_VALUE, NO_VALUE, NO_VALUE),
          Array(NO_VALUE, NO_VALUE, NO_VALUE),
          Array(NO_VALUE, NO_VALUE, NO_VALUE)
        )
      )
    )
  }

  test("should handle valid euclidean vector similarity function calls") {
    // given
    givenGraph {
      val node = tx.createNode(label("Label"))
      node.setProperty("vector", Seq(1.0, 0.0).toArray)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("similarity", "leftNull", "rightNull")
      .projection(
        "vector.similarity.euclidean(args[0], args[1]) AS similarity",
        "vector.similarity.euclidean(NULL, args[1]) AS leftNull",
        "vector.similarity.euclidean(args[0], NULL) AS rightNull"
      )
      .unwind("""[
                |[[1.0, 0.0], [0.0, 0.0]],
                |[[0.0, 0.0], n.vector],
                |[n.vector, [0.0, 0.0]],
                |[n.vector, n.vector],
                |[n.vector, NULL],
                |[NULL, n.vector],
                |[NULL, NULL]
                |] AS args""".stripMargin.replace("\n", ""))
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("similarity", "leftNull", "rightNull").withRows(
      inOrder(
        Seq(
          Array(0.5, NO_VALUE, NO_VALUE),
          Array(0.5, NO_VALUE, NO_VALUE),
          Array(0.5, NO_VALUE, NO_VALUE),
          Array(1.0, NO_VALUE, NO_VALUE),
          Array(NO_VALUE, NO_VALUE, NO_VALUE),
          Array(NO_VALUE, NO_VALUE, NO_VALUE),
          Array(NO_VALUE, NO_VALUE, NO_VALUE)
        )
      )
    )
  }

  test("should respect that toInteger of invalid string returns null") {
    // given, an empty db
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("1 % toInteger(i) AS result")
      .unwind("[1, '2', 'three', null] AS i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("result").withRows(
      singleColumn(
        Seq(
          0,
          1,
          null,
          null
        )
      )
    )
  }

  test("nested OR and AND with potential type failure") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("var1")
      .projection("(false OR (1.sqN < 1 AND false)) AS var1")
      .unwind("[1] AS var0")
      .argument()
      .build()

    execute(query, runtime) should beColumns("var1").withSingleRow(false)
  }
}

// Supported by all runtimes that can deal with changes in the tx-state
trait ExpressionWithTxStateChangesTests[CONTEXT <: RuntimeContext] {
  self: ExpressionTestBase[CONTEXT] =>

  test("hasLabel is false on deleted node") {
    // given
    val node = givenGraph { tx.createNode(label("Label")) }

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
    val nodes = givenGraph {
      nodePropertyGraph(
        1,
        {
          case i: Int => Map("prop" -> i)
        },
        "Label"
      )
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
    an[EntityNotFoundException] should be thrownBy consume(execute(logicalQuery, runtime, input))
  }

  test("should throw if relationship was deleted before accessing relationship property") {
    // given
    val rels = givenGraph {
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
    an[EntityNotFoundException] should be thrownBy consume(execute(logicalQuery, runtime, input))
  }

  test("should handle IN list") {
    // given
    val size = 100
    val input = for (i <- 0 until size) yield Array[Any](list((0 to i).map(intValue): _*))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("5 IN x AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for (i <- 0 until size) yield Array[Any]((0 to i).toArray, i >= 5)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle IN list where list contains nulls") {
    // given
    val size = 100
    val input = for (s <- 0 until size)
      yield Array[Any](list((0 to s).map(i => if (i % 2 == 0) NO_VALUE else intValue(i)): _*))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection("5 IN x AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for (i <- 0 until size) yield Array[Any](if (i >= 5) true else null)
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("hasType on top of expand all") {
    // given
    val size = 100
    givenGraph {
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("A"))
        } else {
          tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("B"))
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasType")
      .projection("r:A AS hasType")
      .expand("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasType").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("hasType on top of expand into") {
    // given
    val size = 100
    givenGraph {
      for (i <- 0 until size) {
        if (i % 2 == 0) {
          tx.createNode(label("START")).createRelationshipTo(
            tx.createNode(label("END")),
            RelationshipType.withName("A")
          )
        } else {
          tx.createNode(label("START")).createRelationshipTo(
            tx.createNode(label("END")),
            RelationshipType.withName("B")
          )
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasType")
      .projection("r:A AS hasType")
      .expandInto("(x)-[r]->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("hasType").withRows(singleColumn((0 until size).map(_ % 2 == 0)))
  }

  test("hasType on top of optional expand all") {
    // given
    val size = 100
    givenGraph {
      for (i <- 0 until size) {
        if (i % 3 == 0) {
          tx.createNode(label("START")).createRelationshipTo(tx.createNode(), RelationshipType.withName("A"))
        } else if (i % 3 == 1) {
          tx.createNode(label("START")).createRelationshipTo(tx.createNode(), RelationshipType.withName("B"))
        } else {
          tx.createNode(label("START"))
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasType")
      .projection("r:A AS hasType")
      .optionalExpandAll("(x)-[r]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("hasType").withRows(singleColumn((0 until size).map {
      case i if i % 3 == 0 => true
      case i if i % 3 == 1 => false
      case i               => null
    }))
  }

  test("hasType on top of optional expand into") {
    // given
    val size = 100
    givenGraph {
      for (i <- 0 until size) {
        if (i % 3 == 0) {
          tx.createNode(label("START")).createRelationshipTo(
            tx.createNode(label("END")),
            RelationshipType.withName("A")
          )
        } else if (i % 3 == 1) {
          tx.createNode(label("START")).createRelationshipTo(
            tx.createNode(label("END")),
            RelationshipType.withName("B")
          )
        } else {
          tx.createNode(label("START"))
          tx.createNode(label("END"))
        }
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasType")
      .projection("r:A AS hasType")
      .optionalExpandInto("(x)-[r]->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("hasType").withRows(singleColumn((0 until size * size).map {
      case i if i >= size  => null
      case i if i % 3 == 0 => true
      case i if i % 3 == 1 => false
      case _               => null
    }))
  }

  test("should handle deleted node with has any label expression") {
    // given
    val node = givenGraph { tx.createNode(label("Label")) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("hasLabel")
      .projection(Map("hasLabel" -> hasAnyLabel("x", "Label")))
      .input(nodes = Seq("x"))
      .build()

    node.delete()
    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(node)))

    // then
    runtimeResult should beColumns("hasLabel").withRows(singleColumn(Seq(false)))
  }

  test("combining scoped expression and runtime constant") {
    // given
    val size = 1024

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filterExpression(and(
        allInList(varFor("a1"), listOf(varFor("x")), trueLiteral),
        RuntimeConstant(varFor("foo"), trueLiteral)
      ))
      .input(variables = Seq("x"))
      .build()
    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to size).map(i => Array[Any](i)): _*))

    // then
    runtimeResult should beColumns("x").withRows(rowCount(size))
  }

  test("should only evaluate rand() once") {
    // given, an empty db
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("number")
      .nonFuseable()
      .unwind("[1,2,3,4,5,6] AS i2")
      .projection("rand() AS number")
      .unwind("[1] AS i1")
      .argument()
      .build()

    val result = consume(execute(logicalQuery, runtime))

    // then
    // should use one and only one random number
    result should have size 6
    result.map(_(0)).toSet should have size 1

  }

  test("should be able to return size of a huge list") {
    // given, an empty db
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("size")
      .projection(s"size(range(1 ,${Long.MaxValue}))  AS size")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    // then
    result should beColumns("size").withSingleRow(Values.longValue(Long.MaxValue))
  }

  test("should not overflow when returning size of a too huge list") {
    // given, an empty db
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("size")
      .projection(s"size(range(0 ,${Long.MaxValue}))  AS size")
      .argument()
      .build()

    // then
    an[org.neo4j.exceptions.ArithmeticException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test("should be able to index into a huge list") {
    // given, an empty db
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("last")
      .projection("range(1 ,$upper)[$upper - 1] AS last")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime, Map("upper" -> Long.MaxValue))

    // then
    result should beColumns("last").withSingleRow(Values.longValue(Long.MaxValue))
  }
}
