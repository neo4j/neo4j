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
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.index.PropertyIndexTestSupport
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.schema.IndexQuery.IndexQueryType.EXACT
import org.neo4j.internal.schema.IndexQuery.IndexQueryType.RANGE
import org.neo4j.kernel.impl.util.ValueUtils.asValue
import org.neo4j.lock.LockType.EXCLUSIVE
import org.neo4j.lock.LockType.SHARED
import org.neo4j.lock.ResourceType.INDEX_ENTRY
import org.neo4j.lock.ResourceType.LABEL
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueCategory
import org.neo4j.values.storable.ValueType
import org.neo4j.values.storable.ValueType.BOOLEAN
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.ValueBooleanLogic

import scala.collection.mutable
import scala.util.Random

// Supported by all runtimes
abstract class NodeIndexSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](runtime = runtime, edition = edition)
    with PropertyIndexTestSupport[CONTEXT]
    with RandomValuesTestSupport {

  // TODO Do we test exact seeks that gets the property without using the index enough?

  testWithIndex(_.supports(EXACT), s"should exact (single) seek nodes of an index with a property") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(randomIndexedNodePropertyGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ???)",
        paramExpr = Some(toExpression(lookFor)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(equalTo(lookFor)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supports(EXACT),
    s"should exact (single) seek nodes of an index with a property with multiple matches"
  ) { index =>
    val numMatches = sizeHint / 5
    val propertyType = randomAmong(index.querySupport(EXACT))
    val propertyValue = randomValue(propertyType)
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop") {
        case (n, i) if i < numMatches => n.setProperty("prop", propertyValue.asObject())
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ???)",
        paramExpr = Some(toExpression(propertyValue)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
  }

  testWithIndex(_.supports(EXACT), "exact single seek should handle null") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = ???)", paramExpr = Some(nullLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(_.supports(EXACT), "should exact (multiple) seek nodes of an index with a property") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val lookFor = Seq(randomAmong(nodes), randomAmong(nodes)).map(n => asValue(n.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ??? OR ???)",
        paramExpr = lookFor.map(toExpression),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(p => lookFor.exists(equalTo(p))))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supports(EXACT), "should handle null in exact multiple seek") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop IN ???)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(_.supports(EXACT), "should handle null in exact multiple seek 2") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val someValues = Seq(randomAmong(nodes), randomAmong(nodes)).map(n => asValue(n.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop IN ???)",
        paramExpr = Some(listOf(toExpression(someValues.head), nullLiteral, toExpression(someValues(1)))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(p => someValues.exists(equalTo(p))))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supports(EXACT), "should handle null in exact multiple seek 3") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val otherPropertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val lookForNode = randomAmong(nodes)
    val lookForExisting = asValue(lookForNode.getProperty("prop"))
    val lookForRandom = randomValue(otherPropertyType)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop IN ???)",
        paramExpr = Some(listOf(toExpression(lookForExisting), nullLiteral, toExpression(lookForRandom))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(p => equalTo(p)(lookForExisting) || equalTo(p)(lookForRandom)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supports(EXACT), "should exact (multiple, but empty) seek nodes of an index with a property") {
    index =>
      val propertyType = randomAmong(index.querySupport(EXACT))
      givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .nodeIndexOperator(
          "x:Honey(prop)",
          customQueryExpression = Some(ManyQueryExpression(listOf())),
          indexType = index.indexType
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(_.supports(EXACT), "should exact (multiple, with null) seek nodes of an index with a property") {
    index =>
      val propertyType = randomAmong(index.querySupport(EXACT))
      val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
      val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .nodeIndexOperator(
          "x:Honey(prop = ??? OR ???)",
          paramExpr = Seq(toExpression(lookFor), nullLiteral),
          indexType = index.indexType
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = nodes.filter(propFilter(equalTo(lookFor)))
      runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supports(EXACT), "should exact (multiple, but identical) seek nodes of an index with a property") {
    index =>
      val propertyType = randomAmong(index.querySupport(EXACT))
      val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
      val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .nodeIndexOperator(
          "x:Honey(prop = ??? OR ???)",
          paramExpr = Seq(toExpression(lookFor), toExpression(lookFor)),
          indexType = index.indexType
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = nodes.filter(propFilter(equalTo(lookFor)))
      runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (single) seek nodes for a value that cannot be indexed"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ???)",
        paramExpr = Some(mapOfInt(("foo", 20))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (multiple) seek nodes for values that cannot be indexed"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    givenGraph(randomIndexedNodePropertyGraph(index.indexType, propertyType))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop IN ???)",
        paramExpr = Some(listOf(mapOfInt(("foo", 20)), mapOfInt(("foo", 30)))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (multiple) seek nodes for values where some cannot be indexed"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(randomIndexedNodePropertyGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop IN ???)",
        paramExpr = Some(listOf(mapOfInt(("foo", 20)), toExpression(lookFor))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(equalTo(lookFor)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supportsUniqueness(EXACT),
    s"should exact seek nodes of a unique index with a property"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultUniqueNodePropertyGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ???)",
        unique = true,
        paramExpr = Some(toExpression(lookFor)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(equalTo(lookFor)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supportsUniqueness(EXACT),
    s"should exact (multiple, but identical) seek nodes of a unique index with a property"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultUniqueNodePropertyGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ??? OR ???)",
        unique = true,
        paramExpr = Seq(toExpression(lookFor), toExpression(lookFor)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(equalTo(lookFor)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supportsUniqueness(EXACT),
    "should exact (multiple, with null) seek nodes of a unique index with a property"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultUniqueNodePropertyGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ??? OR ???)",
        paramExpr = Seq(toExpression(lookFor), nullLiteral),
        unique = true,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(_ == lookFor))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  // RANGE queries
  testWithIndex(_.supports(RANGE), "should seek nodes of an index with a property") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val largerThan = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        s"x:Honey(prop > ???)",
        paramExpr = Some(toExpression(largerThan)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(greaterThan(largerThan)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supports(RANGE), "should seek nodes of an index with a property that cannot be indexed") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        s"x:Honey(prop > ???)",
        paramExpr = Some(mapOfInt(("a", 1))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(_.supports(RANGE, BOOLEAN), s"should handle range seeks: > false") { index =>
    givenGraph {
      nodeIndex(index.indexType, "L", "boolean")
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
      .filter("true")
      .nodeIndexOperator(s"x:L(boolean > ???)", paramExpr = Some(falseLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(3))
  }

  testWithIndex(_.supports(RANGE, BOOLEAN), "should handle range seeks: >= false") { index =>
    givenGraph {
      nodeIndex(index.indexType, "L", "boolean")
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
      .filter("true")
      .nodeIndexOperator(s"x:L(boolean >= ???)", paramExpr = Some(falseLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(5))
  }

  testWithIndex(_.supports(RANGE, BOOLEAN), "should handle range seeks: < false") { index =>
    givenGraph {
      nodeIndex(index.indexType, "L", "boolean")
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
      .filter("true")
      .nodeIndexOperator(s"x:L(boolean < ???)", paramExpr = Some(falseLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(_.supports(RANGE, BOOLEAN), "should handle range seeks: <= false") { index =>
    givenGraph {
      nodeIndex(index.indexType, "L", "boolean")
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
      .filter("true")
      .nodeIndexOperator(s"x:L(boolean <= ???)", paramExpr = Some(falseLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(2))
  }

  testWithIndex(_.supports(RANGE, BOOLEAN), "should handle range seeks: > true") { index =>
    givenGraph {
      nodeIndex(index.indexType, "L", "boolean")
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
      .filter("true")
      .nodeIndexOperator(s"x:L(boolean > ???)", paramExpr = Some(trueLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(_.supports(RANGE, BOOLEAN), "should handle range seeks: >= true") { index =>
    givenGraph {
      nodeIndex(index.indexType, "L", "boolean")
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
      .filter("true")
      .nodeIndexOperator(s"x:L(boolean >= ???)", paramExpr = Some(trueLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(3))
  }

  testWithIndex(_.supports(RANGE, BOOLEAN), "should handle range seeks: < true") { index =>
    givenGraph {
      nodeIndex(index.indexType, "L", "boolean")
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
      .filter("true")
      .nodeIndexOperator(s"x:L(boolean < ???)", paramExpr = Some(trueLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(2))
  }

  testWithIndex(_.supports(RANGE, BOOLEAN), "should handle range seeks: <= true") { index =>
    givenGraph {
      nodeIndex(index.indexType, "L", "boolean")
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", false)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
      tx.createNode(Label.label("L")).setProperty("boolean", true)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(s"x:L(boolean <= ???)", paramExpr = Some(trueLiteral), indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(5))
  }

  testWithIndex(_.supports(RANGE), "should seek nodes with multiple less than bounds") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val someProps = Seq(randomAmong(nodes), randomAmong(nodes)).map(n => asValue(n.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        s"x:Honey(??? > prop < ???)",
        paramExpr = someProps.map(toExpression),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes
      .filter(propFilter(lessThan(someProps.head)))
      .filter(propFilter(lessThan(someProps(1))))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  // Should work also with indexes that don't support range queries
  testWithIndex(
    _.supports(RANGE, ValueType.INT),
    "should seek nodes with multiple less than bounds with different types"
  ) { index =>
    givenGraph {
      nodeGraph(5, "Milk")
      nodeGraph(5, "Honey")
      indexedNodeGraph(index.indexType, "Honey", "prop") { case (node, i) => node.setProperty("prop", i) }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(s"x:Honey(1 > prop < 'foo')", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(_.supports(RANGE), "should seek nodes with multiple less than bounds one inclusive") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val lessThanValue = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        s"x:Honey(??? >= prop < ???)",
        paramExpr = Seq(toExpression(lessThanValue), toExpression(lessThanValue)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(lessThan(lessThanValue)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supports(RANGE), "should seek nodes with multiple greater than bounds") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val greaterThanValue = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        s"x:Honey(??? < prop > ???)",
        paramExpr = Seq(toExpression(greaterThanValue), toExpression(greaterThanValue)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(greaterThan(greaterThanValue)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected, listInAnyOrder = true))
  }

  testWithIndex(_.supports(RANGE), "should seek nodes with multiple greater than bounds with different types") {
    index =>
      val propertyType = randomAmong(index.querySupport(RANGE))
      givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .nodeIndexOperator(s"x:Honey(1 < prop > 'foo')", indexType = index.indexType)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(_.supports(RANGE), "should seek nodes with multiple greater than bounds one inclusive") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val someProperty = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        s"x:Honey(??? <= prop > ???)",
        paramExpr = Seq(toExpression(someProperty), toExpression(someProperty)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(greaterThan(someProperty)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected, listInAnyOrder = true))
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 10 == 0 =>
          node.setProperty("prop", i)
          node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = 10, prop2 = '10')", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(nodes(10 / 10))
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.TEXT, ValueCategory.TEXT),
    "should support composite index (multiple results)"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 2 == 0 =>
          node.setProperty("prop", (i % 5).toString)
          node.setProperty("prop2", (i % 3).toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = '0', prop2 = '0')", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(n => n.getProperty("prop") == "0" && n.getProperty("prop2") == "0")
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index (multiple values)"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 10 == 0 =>
          node.setProperty("prop", i)
          node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = 10 OR 20, prop2 = '10' OR '30')", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(nodes(10 / 10))
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index with equality and existence check"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 3 == 0 =>
          node.setProperty("prop", i)
          node.setProperty("prop2", i.toString)
        case (node, i) =>
          node.setProperty("prop", i % 20)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = 10, prop2)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(n => n.hasProperty("prop2") && n.getProperty("prop") == 10)
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index seeks for values that cannot be indexed"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 10 == 0 =>
          node.setProperty("prop", i)
          node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop IN ???, prop2 = '10')",
        indexType = index.indexType,
        paramExpr = Some(listOf(mapOfInt(("foo", 20)), literalInt(10)))
      ).build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(nodes(10 / 10))
  }

  testWithIndex(
    _.supportsComposite(RANGE, ValueCategory.TEXT, ValueCategory.TEXT),
    "should support composite index with equality and range check"
  ) { index =>
    givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) =>
          node.setProperty("prop", i.toString)
          node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = '10', prop2 > '10')", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(
    _.supportsComposite(RANGE, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index with range check and existence check"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) =>
          node.setProperty("prop", i)
          if (i % 3 == 0) node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop > 10, prop2)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(n => n.getProperty("prop").asInstanceOf[Int] > 10 && n.hasProperty("prop2"))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index seek with null"
  ) { index =>
    givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 10 == 0 =>
          node.setProperty("prop", i)
          node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = 10, prop2 = ???)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(
    _.supportsComposite(RANGE, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index seek with null 2"
  ) { index =>
    givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 10 == 0 =>
          node.setProperty("prop", i)
          node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = 10, prop2 > ???)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index (multiple values) and null"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 10 == 0 =>
          node.setProperty("prop", i)
          node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = 10 OR ???, prop2)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(nodes(10 / 10))
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support composite index with duplicated seek term"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 10 == 0 =>
          node.setProperty("prop", i)
          node.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop IN ???, prop2 = '10')",
        indexType = index.indexType,
        paramExpr = Some(listOfInt(10, 10))
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(nodes(10 / 10))
  }

  testWithIndex(_.supports(EXACT), "should cache properties with exact seek") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cache[x.prop] AS prop")
      .nodeIndexOperator(
        "x:Honey(prop = ???)",
        paramExpr = Some(toExpression(lookFor)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(_ == lookFor)).map(n => Array[Object](n, lookFor))
    runtimeResult should beColumns("x", "prop").withRows(expected)
  }

  testWithIndex(_.supportsValues(RANGE), "should cache properties") { index =>
    val propertyType = randomAmong(index.provideValueSupport(RANGE))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val someProperty = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cache[x.prop] AS prop")
      .nodeIndexOperator(
        s"x:Honey(prop > ???)",
        getValue = _ => GetValue,
        paramExpr = Some(toExpression(someProperty)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes
      .filter(propFilter(greaterThan(someProperty)))
      .map(n => Array(n, n.getProperty("prop")))
    runtimeResult should beColumns("x", "prop").withRows(expected, listInAnyOrder = true)
  }

  testWithIndex(
    i => i.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT) && i.supportsValues(EXACT),
    "should cache properties in composite index"
  ) { index =>
    val type1 = randomAmong(index.provideValueSupport(EXACT))
    val type2 = randomAmong(index.provideValueSupport(EXACT))
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop", "prop2") {
        case (node, i) if i % 10 == 0 =>
          node.setProperty("prop", randomValue(type1).asObject())
          node.setProperty("prop2", randomValue(type2).asObject())
      }
    }
    val someNode = randomAmong(nodes)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop", "prop2")
      .projection("cache[x.prop] AS prop", "cache[x.prop2] AS prop2")
      .nodeIndexOperator(
        "x:Honey(prop = ???, prop2 = ???)",
        getValue = _ => GetValue,
        paramExpr = Seq(someNode.getProperty("prop"), someNode.getProperty("prop2")).map(toExpression),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedProp = Values.of(someNode.getProperty("prop"))
    val expectedProp2 = Values.of(someNode.getProperty("prop2"))
    val expected = nodes
      .map(n => (n, Values.of(n.getProperty("prop")), Values.of(n.getProperty("prop2"))))
      .collect {
        case (n, prop, prop2) if prop == expectedProp && prop2 == expectedProp2 =>
          Array[Any](n, prop, prop2)
      }
    runtimeResult should beColumns("x", "prop", "prop2").withRows(expected)
  }

  testWithIndex(_.supportsValues(EXACT), "should use existing values from arguments when available") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val values = Seq(randomAmong(nodes), randomAmong(nodes)).map(n => asValue(n.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.nodeIndexOperator(
        "x:Honey(prop = ???)",
        getValue = _ => GetValue,
        paramExpr = Some(varFor("value")),
        argumentIds = Set("value"),
        indexType = index.indexType
      )
      .input(variables = Seq("value"))
      .build()

    val input = inputValues(values.map(v => Array[Any](v)): _*)

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = values.flatMap { v =>
      nodes.filter(propFilter(equalTo(v)))
    }
    runtimeResult should beColumns("x").withRows(singleColumn(expected, listInAnyOrder = true))
  }

  testWithIndex(
    _.supportsOrderDesc(RANGE),
    "should seek nodes of an index with a property in ascending order"
  ) { index =>
    // parallel does not maintain order
    assume(!isParallel)
    val propertyType = randomAmong(index.orderAscSupport(RANGE))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("x.prop AS result")
      .nodeIndexOperator(
        s"x:Honey(prop > ???)",
        paramExpr = Some(toExpression(someProp)),
        indexOrder = IndexOrderAscending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes
      .filter(propFilter(greaterThan(someProp)))
      .sorted(propOrdering)
      .map(_.getProperty("prop"))
    runtimeResult should beColumns("result").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderDesc(RANGE),
    "should seek nodes of an index with a property in descending order"
  ) { index =>
    // parallel does not maintain order
    assume(!isParallel)
    val propertyType = randomAmong(index.orderDescSupport(RANGE))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("x.prop AS result")
      .nodeIndexOperator(
        s"x:Honey(prop > ???)",
        paramExpr = Some(toExpression(someProp)),
        indexOrder = IndexOrderDescending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes
      .filter(propFilter(greaterThan(someProp)))
      .sorted(propOrdering.reverse)
      .map(_.getProperty("prop"))

    runtimeResult should beColumns("result").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(_.supportsOrderAsc(EXACT), "should handle order in multiple index seek, int ascending") { index =>
    val propertyType = randomAmong(index.orderAscSupport(EXACT))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val someValues =
      Seq(randomAmong(nodes), randomAmong(nodes), randomAmong(nodes)).map(n => asValue(n.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .nodeIndexOperator(
        "x:Honey(prop)",
        customQueryExpression = Some(ManyQueryExpression(listOf(someValues.map(toExpression): _*))),
        indexOrder = IndexOrderAscending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes
      .map(n => asValue(n.getProperty("prop")))
      .filter(p => someValues.exists(equalTo(p)))
      .sorted(ANY_VALUE_ORDERING)
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(_.supportsOrderDesc(EXACT), "should handle order in multiple index seek, int descending") { index =>
    val propertyType = randomAmong(index.orderDescSupport(EXACT))
    val nodes = givenGraph(defaultRandomIndexedNodePropertyGraph(index.indexType, propertyType))
    val someProps =
      Seq(randomAmong(nodes), randomAmong(nodes), randomAmong(nodes)).map(n => asValue(n.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .nodeIndexOperator(
        "x:Honey(prop)",
        customQueryExpression = Some(ManyQueryExpression(listOf(someProps.map(toExpression): _*))),
        indexOrder = IndexOrderDescending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes
      .filter(propFilter(someProps.contains))
      .map(_.getProperty("prop"))
      .sorted(propValueOrdering.reverse)
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderDesc(EXACT, ValueType.STRING),
    "should handle order in multiple index seek, string descending"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop") {
        case (node, i) => node.setProperty("prop", (i % 10).toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop AS prop")
      .nodeIndexOperator(
        "x:Honey(prop)",
        customQueryExpression = Some(ManyQueryExpression(listOf(literal("7"), literal("2"), literal("3")))),
        indexOrder = IndexOrderDescending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes
      .collect(collectProp[String](Set("7", "2", "3").contains))
      .sorted(Ordering.String.reverse)
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  // array properties
  testWithIndex(_.supports(EXACT), "should handle multiple index seek with overflowing morsels") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    assume(index.supports(EXACT, propertyType))
    val value1 = randomValue(propertyType)
    val value2 = randomValue(propertyType)
    // given
    givenGraph {
      indexedNodeGraph(index.indexType, "A", "prop") { case (node, i) =>
        if (i % 2 == 0) node.setProperty("prop", value1.asObject())
        else node.setProperty("prop", value2.asObject())
        node.createRelationshipTo(tx.createNode(), RelationshipType.withName("R1"))
        node.createRelationshipTo(tx.createNode(), RelationshipType.withName("R2"))
        node.createRelationshipTo(tx.createNode(), RelationshipType.withName("R3"))
        node.createRelationshipTo(tx.createNode(), RelationshipType.withName("R4"))
        node.createRelationshipTo(tx.createNode(), RelationshipType.withName("R5"))
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expandAll("(x)-->(y)")
      .nodeIndexOperator(
        "x:A(prop = ??? OR ???)",
        paramExpr = Seq(toExpression(value1.asObject()), toExpression(value2.asObject())),
        indexType = index.indexType
      )
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint * 5))
  }

  testWithIndex(
    _.supports(EXACT, ValueType.INT_ARRAY),
    "should exact (single) seek nodes of an index with an array property"
  ) { index =>
    val nodes = givenGraph {
      nodeGraph(5, "Milk")
      indexedNodeGraph(index.indexType, "Honey", "prop") {
        case (node, i) if i % 10 == 0 => node.setProperty("prop", Array[Int](i))
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ???)",
        paramExpr = Some(listOf(literalInt(20))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(nodes(20 / 10))
  }

  testWithIndex(
    _.supportsUniqueness(RANGE),
    "should seek nodes of a unique index with a property"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val nodes = givenGraph(defaultUniqueNodePropertyGraph(index.indexType, propertyType))
    val someProp = Values.of(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        s"x:Honey(prop > ???)",
        paramExpr = Some(toExpression(someProp)),
        unique = true,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(greaterThan(someProp)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supportsUniqueness(EXACT), "should multi seek nodes of a unique index with property") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(defaultUniqueNodePropertyGraph(index.indexType, propertyType))
    val someRandomProps = Range(0, 3).map(_ => randomValue(propertyType))
    val someExistingProps = Seq(randomAmong(nodes), randomAmong(nodes), randomAmong(nodes))
      .map(n => asValue(n.getProperty("prop")))
    val someProps = shuffle(someRandomProps ++ someExistingProps)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop)",
        customQueryExpression = Some(ManyQueryExpression(listOf(someProps.map(toExpression): _*))),
        unique = true,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(someProps.contains))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should work with multiple index types") {
    val nodes = givenGraph {
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
      .produceResults("x")
      .union()
      .|.nodeIndexOperator("x:Label(prop > 42)", indexType = IndexType.RANGE)
      .nodeIndexOperator("x:Label(prop CONTAINS '1')", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter { n =>
      n.getProperty("prop") match {
        case s: String  => s.contains("1")
        case i: Integer => i > 42
        case _          => false
      }
    }

    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supports(EXACT), s"should support filter in in the same pipeline") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val nodes = givenGraph(randomIndexedNodePropertyGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(nodes).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("1 IN [1,2,3]")
      .nodeIndexOperator(
        "x:Honey(prop = ???)",
        paramExpr = Some(toExpression(lookFor)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(propFilter(equalTo(lookFor)))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supports(EXACT, ValueType.STRING_ARRAY), s"should exact (single) seek an empty array") { index =>
    val n = givenGraph {
      nodeIndex(index.indexType, "Honey", "prop")
      val n = tx.createNode(Label.label("Honey"))
      n.setProperty("prop", Array.empty[String])
      n
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop = ???)",
        indexType = index.indexType,
        paramExpr = Some(listOf())
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleRow(n)
  }

  private def propFilter(predicate: Value => Boolean): Node => Boolean = {
    n => n.hasProperty("prop") && predicate.apply(asValue(n.getProperty("prop")))
  }

  private def equalTo(rhs: Value)(lhs: Value): Boolean = ValueBooleanLogic.equals(lhs, rhs) == BooleanValue.TRUE

  private def greaterThan(rhs: Value)(lhs: Value): Boolean =
    ValueBooleanLogic.greaterThan(lhs, rhs) == BooleanValue.TRUE
  private def lessThan(rhs: Value)(lhs: Value): Boolean = ValueBooleanLogic.lessThan(lhs, rhs) == BooleanValue.TRUE

  private def collectProp[T](predicate: T => Boolean): PartialFunction[Node, T] = {
    case n if predicate(n.getProperty("prop").asInstanceOf[T]) => n.getProperty("prop").asInstanceOf[T]
  }

  def propOrdering: Ordering[Node] =
    Ordering.by[Node, AnyValue](n => Values.of(n.getProperty("prop")).asInstanceOf[AnyValue])(ANY_VALUE_ORDERING)

  def propValueOrdering: Ordering[Any] =
    Ordering.by[Any, AnyValue](v => Values.of(v).asInstanceOf[AnyValue])(ANY_VALUE_ORDERING)

  def randomIndexedNodePropertyGraph(indexType: IndexType, propertyType: ValueType): Seq[Node] = {
    indexedNodeGraph(indexType, "Honey", "prop") {
      case (n, i) if i % 10 == 0 => n.setProperty("prop", randomValue(propertyType).asObject())
    }
  }

  def defaultRandomIndexedNodePropertyGraph(indexType: IndexType, propertyType: ValueType): Seq[Node] = {
    indexedNodeGraph(indexType, "Honey", "prop") {
      case (n, _) => n.setProperty("prop", randomValue(propertyType).asObject())
    }
  }

  def defaultUniqueNodePropertyGraph(indexType: IndexType, propertyType: ValueType): Seq[Node] = {
    nodeGraph(5, "Milk") // Unrelated label
    val usedValues = mutable.Set.empty[Value]
    val nodes = uniqueIndexedNodeGraph(indexType, "Honey", "prop") {
      case (n, i) if i % 10 == 0 =>
        val value = randomValue(propertyType)
        if (!usedValues.contains(value)) {
          usedValues += value
          n.setProperty("prop", value.asObject())
        }
    }
    nodes.filter(_.hasProperty("prop"))
  }

  def indexedNodeGraph(
    indexType: IndexType,
    indexedLabel: String,
    indexedProperties: String*
  )(propFunction: PartialFunction[(Node, Int), Unit]): Seq[Node] = {
    nodeIndex(indexType, indexedLabel, indexedProperties: _*)
    nodeGraph(sizeHint, indexedLabel).zipWithIndex.collect {
      case t @ (n, _) if propFunction.isDefinedAt(t) =>
        propFunction.apply(t)
        n
    }
  }

  def uniqueIndexedNodeGraph(
    indexType: IndexType,
    indexedLabel: String,
    indexedProperties: String*
  )(propFunction: PartialFunction[(Node, Int), Unit]): Seq[Node] = {
    uniqueNodeIndex(indexType, indexedLabel, indexedProperties: _*)
    nodeGraph(sizeHint, indexedLabel).zipWithIndex.collect {
      case t @ (n, _) if propFunction.isDefinedAt(t) =>
        propFunction.apply(t)
        n
    }
  }
}

// Supported by interpreted, slotted, pipelined, not by parallel
trait NodeLockingUniqueIndexSeekTestBase[CONTEXT <: RuntimeContext] {
  self: NodeIndexSeekTestBase[CONTEXT] =>

  test("should grab shared lock when finding a node") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(s"x:Honey(prop = $propToFind)", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(propToFind)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should grab shared lock when finding a node (multiple properties)") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop1", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> s"$i")
        },
        "Honey"
      )
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(s"x:Honey(prop1 = $propToFind, prop2 = '$propToFind')", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(propToFind)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should grab an exclusive lock when not finding a node") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    val propToFind = sizeHint + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(s"x:Honey(prop = $propToFind)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows().withLocks((EXCLUSIVE, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should not grab any lock when readOnly = true") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(s"x:Honey(prop = $propToFind)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(propToFind)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks()
  }

  test("should exact seek nodes of a locking unique index with a property") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = 20)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should exact seek nodes of a locking composite unique index with properties") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop1", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop1" -> i, "prop2" -> s"$i")
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop1 = 20, prop2 = '20')", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should exact (multiple, but identical) seek nodes of a locking unique index with a property") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = 20 OR 20)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20))
    runtimeResult should beColumns("x").withRows(singleColumn(expected)).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, LABEL)
    )
  }

  test("should exact seek nodes of a composite unique index with properties") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop1", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop1" -> i, "prop2" -> s"$i")
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop1 = 20, prop2 = '20')", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should seek nodes of a composite unique index with properties") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop1", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop1" -> i, "prop2" -> s"$i")
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(s"x:Honey(prop1 > ${sizeHint / 2}, prop2)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.filter { case (_, i) => i % 10 == 0 && i > sizeHint / 2 }.map(_._1)
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should exact (multiple, not identical) seek nodes of a locking unique index with a property") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = 20 OR 30)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(nodes(20), nodes(30))
    runtimeResult should beColumns("x").withRows(singleColumn(expected)).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, INDEX_ENTRY),
      (SHARED, LABEL)
    )
  }

  test("should support composite index and unique locking") {
    val nodes = givenGraph {
      nodeIndex(IndexType.RANGE, "Honey", "prop", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = 10, prop2 = '10')", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(10)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should support composite unique index and unique locking") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop = 10, prop2 = '10')", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(10)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should cache properties in locking unique index") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cache[x.prop] AS prop")
      .nodeIndexOperator("x:Honey(prop = 10)", _ => GetValue, unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "prop").withSingleRow(nodes(10), 10).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, LABEL)
    )
  }

  test("should verify that two nodes are identical with locking reads") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:Honey(prop = 20)", unique = true)
      .nodeIndexOperator("x:Honey(prop = 20)", unique = true)
      .build(false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test(s"should multi seek nodes of a unique index with locking") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodePropertyGraph(sizeHint, { case i if i % 10 == 0 => Map("prop" -> i) }, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator(
        "x:Honey(prop)",
        customQueryExpression = Some(ManyQueryExpression(listOf(Seq(-1L, 0L, 1L, 10L, 20L).map(literalInt(_)): _*))),
        unique = true
      )
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedLocks = Seq(
      (EXCLUSIVE, INDEX_ENTRY),
      (EXCLUSIVE, INDEX_ENTRY),
      (SHARED, INDEX_ENTRY),
      (SHARED, INDEX_ENTRY),
      (SHARED, INDEX_ENTRY),
      (SHARED, LABEL)
    )
    val expected = Seq(Array(nodes.head), Array(nodes(10)), Array(nodes(20)))
    runtimeResult should beColumns("x").withRows(expected).withLocks(expectedLocks: _*)
  }
}

// Supported by slotted, pipelined, parallel (not compiled though because of composite index)
trait EnterpriseNodeIndexSeekTestBase[CONTEXT <: RuntimeContext] {
  self: NodeIndexSeekTestBase[CONTEXT] =>

  test("should support composite index with equality and equality check on the RHS of Apply with Node Key constraint") {
    val (milk, honey) = givenGraph {
      nodeKey("Honey", "prop", "prop2")
      val milk = nodeGraph(5, "Milk")
      val honey = nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i, "prop2" -> i.toString)
        },
        "Honey"
      )
      (milk, honey)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("'borked' AS borked")
      .apply()
      .|.nodeIndexOperator(
        "x:Honey(prop = 10, prop2 = '10')",
        unique = true,
        getValue = _ => GetValue,
        argumentIds = Set("a")
      )
      .distinct("a AS a")
      .nodeByLabelScan("a", "Milk", IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected: Seq[Node] = for {
      _ <- milk
    } yield honey(10)
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }
}

// Supported by slotted, pipelined, parallel (not compiled though because of composite index)
trait SerialEnterpriseNodeIndexSeekTestBase[CONTEXT <: RuntimeContext] {
  self: NodeIndexSeekTestBase[CONTEXT] =>

  test("should handle null in exact unique multiple seek") {
    givenGraph {
      nodeKey("Honey", "prop1", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .nodeIndexOperator("x:Honey(prop1 = 10, prop2 = ???)", unique = true, paramExpr = Some(nullLiteral))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }
}

abstract class ParallelNodeIndexSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends NodeIndexSeekTestBase[CONTEXT](edition, runtime, sizeHint) {
  override def supportedPropertyTypes(): Seq[ValueType] = parallelSupportedTypes
}
