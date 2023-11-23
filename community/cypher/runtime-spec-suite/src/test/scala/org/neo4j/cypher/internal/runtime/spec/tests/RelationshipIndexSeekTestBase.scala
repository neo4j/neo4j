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
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.index.PropertyIndexTestSupport
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.schema.IndexQuery.IndexQueryType.EXACT
import org.neo4j.internal.schema.IndexQuery.IndexQueryType.RANGE
import org.neo4j.kernel.impl.util.ValueUtils.asValue
import org.neo4j.lock.LockType.EXCLUSIVE
import org.neo4j.lock.LockType.SHARED
import org.neo4j.lock.ResourceType.INDEX_ENTRY
import org.neo4j.lock.ResourceType.RELATIONSHIP_TYPE
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueCategory
import org.neo4j.values.storable.ValueType
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.ValueBooleanLogic

import scala.util.Random

// Supported by all runtimes
abstract class RelationshipIndexSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](runtime = runtime, edition = edition)
    with PropertyIndexTestSupport[CONTEXT]
    with RandomValuesTestSupport {

  testWithIndex(_.supports(EXACT), "should exact (single) directed relationship seek of an index with a property") {
    index =>
      val propertyType = randomAmong(index.querySupport(EXACT))
      val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
      val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          "(x)-[r:R(prop = ???)]->(y)",
          paramExpr = Some(toExpression(lookFor)),
          indexType = index.indexType
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter(equalTo(lookFor))).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(_.supports(EXACT), "should exact (single) undirected relationship seek of an index with a property") {
    index =>
      val propertyType = randomAmong(index.querySupport(EXACT))
      val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
      val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          "(x)-[r:R(prop = ???)]-(y)",
          paramExpr = Some(toExpression(lookFor)),
          indexType = index.indexType
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter(equalTo(lookFor))).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (single) directed seek nodes of an index with a property with multiple matches"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val propertyValues = randomValues(5, propertyType)
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop") {
        case (r, _) => r.setProperty("prop", randomAmong(propertyValues).asObject())
      }
    }

    val lookFor = propertyValues.head

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = ???)]->(y)",
        paramExpr = Some(toExpression(lookFor)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(equalTo(lookFor))).map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (single) undirected seek nodes of an index with a property with multiple matches"
  ) { index =>
    val propertyValues = randomValues(5, index.querySupport(EXACT): _*)
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop") {
        case (r, _) => r.setProperty("prop", randomAmong(propertyValues).asObject())
      }
    }

    val lookFor = propertyValues.head

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = ???)]-(y)",
        paramExpr = Some(toExpression(lookFor)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(equalTo(lookFor))).flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(_.supports(EXACT), "exact single directed seek should handle null") { index =>
    val types = index.querySupport(EXACT)
    givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop") {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", randomValue(randomAmong(types)).asObject())
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = ???)]->(y)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(_.supports(EXACT), "exact single undirected seek should handle null") { index =>
    val types = index.querySupport(EXACT)
    givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop") {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", randomValue(randomAmong(types)).asObject())
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = ???)]-(y)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(_.supports(EXACT), "should exact (multiple) directed seek nodes of an index with a property") { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))

    val lookFor = Seq(randomAmong(relationships), randomAmong(relationships)).map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        indexSeekString = "(x)-[r:R(prop)]->(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(lookFor.map(toExpression): _*))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(p => lookFor.exists(equalTo(p)))).map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(_.supports(EXACT), "should exact (multiple) undirected seek nodes of an index with a property") {
    index =>
      val propertyType = randomAmong(index.querySupport(EXACT))
      val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))

      val lookFor = Seq(randomAmong(relationships), randomAmong(relationships)).map(r => asValue(r.getProperty("prop")))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop)]-(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(lookFor.map(toExpression): _*))),
          indexType = index.indexType
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected =
        relationships.filter(propFilter(p => lookFor.exists(equalTo(p)))).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(_.supports(EXACT), "should handle null in exact multiple directed seek") { index =>
    val types = index.querySupport(EXACT)
    givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop") {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", randomValue(randomAmong(types)).asObject())
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop IN ???)]->(y)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(_.supports(EXACT), "should handle null in exact multiple undirected seek") { index =>
    val types = index.querySupport(EXACT)
    givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop") {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", randomValue(randomAmong(types)).asObject())
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop IN ???)]-(y)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (multiple, but empty) directed seek relationships of an index with a property"
  ) { index =>
    val types = index.querySupport(EXACT)
    givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop") {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", randomValue(randomAmong(types)).asObject())
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop)]->(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf())),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (multiple, but empty) undirected seek relationships of an index with a property"
  ) { index =>
    givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop") {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop)]-(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf())),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (multiple, with null) directed seek relationships of an index with a property"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))

    val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        indexSeekString = "(x)-[r:R(prop)]->(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), nullLiteral))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(equalTo(lookFor))).map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (multiple, with null) undirected seek relationships of an index with a property"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        indexSeekString = "(x)-[r:R(prop)]-(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), nullLiteral))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(equalTo(lookFor))).flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact seek for value that cannot be indexed"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        indexSeekString = "(x)-[r:R(prop)]-(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), mapOfInt(("foo", 42))))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(equalTo(lookFor))).flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(_.indexType == IndexType.RANGE, "should support directed seek on composite index with random type") {
    index =>
      val propertyType1 = randomAmong(index.querySupport(EXACT))
      val propertyType2 = randomAmong(index.querySupport(EXACT))
      val relationships = givenGraph {
        indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", randomValue(propertyType1).asObject())
            r.setProperty("prop2", randomValue(propertyType2).asObject())
        }
      }

      val lookForRelationship = randomAmong(relationships)
      val lookFor1 = asValue(lookForRelationship.getProperty("prop"))
      val lookFor2 = asValue(lookForRelationship.getProperty("prop2"))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop, prop2)]->(y)",
          customQueryExpression = Some(CompositeQueryExpression(Seq(
            SingleQueryExpression(toExpression(lookFor1)),
            SingleQueryExpression(toExpression(lookFor2))
          ))),
          indexType = index.indexType
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .filter(propFilter(equalTo(lookFor1)))
        .filter(propertyFilter("prop2")(equalTo(lookFor2)))
        .map(r => Array(r.getStartNode, r, r.getEndNode))
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (multiple, but identical) directed seek relationships of an index with a property"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        indexSeekString = "(x)-[r:R(prop)]->(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), toExpression(lookFor)))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(equalTo(lookFor))).map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supports(EXACT),
    "should exact (multiple, but identical) undirected seek relationships of an index with a property"
  ) { index =>
    val propertyType = randomAmong(index.querySupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        indexSeekString = "(x)-[r:R(prop)]-(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), toExpression(lookFor)))),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(equalTo(lookFor))).flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  // RANGE queries
  testWithIndex(_.supports(RANGE), "should directed seek relationships of an index with a property") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        s"(x)-[r:R(prop > ???)]->(y)",
        paramExpr = Some(toExpression(someProp)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(greaterThan(someProp))).map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(_.supports(RANGE), "should undirected seek relationships of an index with a property") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        s"(x)-[r:R(prop > ???)]-(y)",
        paramExpr = Some(toExpression(someProp)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(greaterThan(someProp))).flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(_.supports(RANGE), "should directed seek relationships with multiple less than bounds") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProps = Seq(randomAmong(relationships), randomAmong(relationships)).map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        s"(x)-[r:R(??? > prop < ???)]->(y)",
        paramExpr = someProps.map(toExpression),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(lessThan(someProps.head)))
      .filter(propFilter(lessThan(someProps(1))))
      .map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(_.supports(RANGE), "should undirected seek relationships with multiple less than bounds") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProps = Seq(randomAmong(relationships), randomAmong(relationships)).map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(??? > prop < ???)]-(y)",
        paramExpr = someProps.map(toExpression),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(lessThan(someProps.head)))
      .filter(propFilter(lessThan(someProps(1))))
      .flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    i => i.supports(RANGE, ValueType.STRING, ValueType.INT),
    "should directed seek relationships with multiple less than bounds with different types"
  ) { index =>
    val propertyType = randomAmong(Seq(ValueType.STRING, ValueType.INT))
    givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 'foo')]->(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(
    _.supports(RANGE, ValueType.STRING, ValueType.INT),
    "should undirected seek relationships with multiple less than bounds with different types"
  ) { index =>
    val propertyType = randomAmong(Seq(ValueType.STRING, ValueType.INT))
    givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 'foo')]-(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support directed seek on composite index"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 10 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]->(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodesAndRelationship(relationships(10 / 10))
    runtimeResult should beColumns("x", "r", "y").withSingleRow(expected: _*)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support undirected seek on composite index"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 10 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]-(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodesAndRelationshipUndirectional(relationships(10 / 10))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.NUMBER),
    "should support directed seek on composite index (multiple results)"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 2 == 0 =>
          r.setProperty("prop", i % 5)
          r.setProperty("prop2", i % 3)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 0, prop2 = 0)]->(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .collect { case r if r.getProperty("prop") == 0 && r.getProperty("prop2") == 0 => nodesAndRelationship(r) }
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.TEXT, ValueCategory.TEXT),
    "should support undirected seek on composite index (multiple results)"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 2 == 0 =>
          r.setProperty("prop", (i % 5).toString)
          r.setProperty("prop2", (i % 3).toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = '0', prop2 = '0')]-(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(r => r.getProperty("prop") == "0" && r.getProperty("prop2") == "0")
      .flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support directed seek on composite index (multiple values)"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 10 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10 OR 20, prop2 = '10' OR '30')]->(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10 / 10)
    runtimeResult should beColumns("x", "r", "y").withSingleRow(expected.getStartNode, expected, expected.getEndNode)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support undirected seek on composite index (multiple values)"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 10 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10 OR 20, prop2 = '10' OR '30')]-(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodesAndRelationshipUndirectional(relationships(10 / 10))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support directed composite index seek with equality and existence check"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 3 == 0 =>
          r.setProperty("prop", i % 20)
          r.setProperty("prop2", i.toString)
        case (r, i) =>
          r.setProperty("prop", i % 20)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2)]->(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(n => n.hasProperty("prop2") && n.getProperty("prop").asInstanceOf[Int] == 10)
      .map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support undirected composite index seek with equality and existence check"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 3 == 0 =>
          r.setProperty("prop", i % 20)
          r.setProperty("prop2", i.toString)
        case (r, i) =>
          r.setProperty("prop", i % 20)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2)]-(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(n => n.hasProperty("prop2") && n.getProperty("prop").asInstanceOf[Int] == 10)
      .flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(RANGE, ValueCategory.NUMBER, ValueCategory.NUMBER),
    "should support directed composite index seek with equality and range check"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) =>
          r.setProperty("prop", i % 20)
          r.setProperty("prop2", i)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 > 10)]->(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(n => n.getProperty("prop") == 10 && n.getProperty("prop2").asInstanceOf[Int] > 10)
      .map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(RANGE, ValueCategory.NUMBER, ValueCategory.NUMBER),
    "should support undirected composite index seek with equality and range check"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) =>
          r.setProperty("prop", i % 20)
          r.setProperty("prop2", i)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 > 10)]-(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(n => n.getProperty("prop") == 10 && n.getProperty("prop2").asInstanceOf[Int] > 10)
      .flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(RANGE, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support directed seek on composite index with range check and existence check"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 3 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        case (r, i) =>
          r.setProperty("prop", i)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop > 10, prop2)]->(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(n => n.getProperty("prop").asInstanceOf[Int] > 10 && n.hasProperty("prop2"))
      .map(nodesAndRelationship)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(RANGE, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support undirected seek on composite index with range check and existence check"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 3 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        case (r, i) =>
          r.setProperty("prop", i)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop > 10, prop2)]-(y)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(n => n.getProperty("prop").asInstanceOf[Int] > 10 && n.hasProperty("prop2"))
      .flatMap(nodesAndRelationshipUndirectional)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support null in directed seek on composite index"
  ) { index =>
    givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 10 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = 10, prop2 = ???)]->(y)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "should support null in undirected seek on composite index"
  ) { index =>
    givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 10 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = 10, prop2 = ???)]-(y)",
        paramExpr = Some(nullLiteral),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  testWithIndex(_.supportsValues(EXACT), "directed exact seek should cache properties") { index =>
    val propertyType = randomAmong(index.provideValueSupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = ???)]->(y)",
        paramExpr = Some(toExpression(lookFor)),
        getValue = _ => GetValue,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(propFilter(equalTo(lookFor))).map(r => Array[Object](r, lookFor))
    runtimeResult should beColumns("r", "prop").withRows(inAnyOrder(expected))
  }

  testWithIndex(_.supportsValues(EXACT), "undirected exact seek should cache properties") { index =>
    val propertyType = randomAmong(index.provideValueSupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val lookFor = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = ???)]-(y)",
        paramExpr = Some(toExpression(lookFor)),
        getValue = _ => GetValue,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(equalTo(lookFor)))
      .flatMap(r => Seq(r, r))
      .map(r => Array(r, r.getProperty("prop")))
    runtimeResult should beColumns("r", "prop").withRows(inAnyOrder(expected))
  }

  testWithIndex(_.supportsValues(RANGE), "directed seek should cache properties") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .relationshipIndexOperator(
        s"(x)-[r:R(prop > ???)]->(y)",
        paramExpr = Some(toExpression(someProp)),
        getValue = _ => GetValue,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(greaterThan(someProp)))
      .map(r => Array(r, r.getProperty("prop")))
    runtimeResult should beColumns("r", "prop").withRows(expected)
  }

  testWithIndex(_.supportsValues(RANGE), "undirected seek should cache properties") { index =>
    val propertyType = randomAmong(index.querySupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .relationshipIndexOperator(
        "(x)-[r:R(prop > ???)]-(y)",
        paramExpr = Some(toExpression(someProp)),
        getValue = _ => GetValue,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(greaterThan(someProp)))
      .map(r => Array(r, r.getProperty("prop")))
      .flatMap(r => Seq(r, r))
    runtimeResult should beColumns("r", "prop").withRows(expected)
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "directed composite seek should cache properties"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 10 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop", "prop2")
      .projection("cacheR[r.prop] AS prop", "cacheR[r.prop2] AS prop2")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = 10, prop2 = '10')]->(y)",
        getValue = _ => GetValue,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "prop", "prop2").withSingleRow(relationships(10 / 10), 10, "10")
  }

  testWithIndex(
    _.supportsComposite(EXACT, ValueCategory.NUMBER, ValueCategory.TEXT),
    "undirected composite seek should cache properties"
  ) { index =>
    val relationships = givenGraph {
      indexedCircleGraph(index.indexType, "R", "prop", "prop2") {
        case (r, i) if i % 10 == 0 => {
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop", "prop2")
      .projection("cacheR[r.prop] AS prop", "cacheR[r.prop2] AS prop2")
      .relationshipIndexOperator(
        "(x)-[r:R(prop = 10, prop2 = '10')]-(y)",
        getValue = _ => GetValue,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "prop", "prop2").withRows(Seq(
      Array[Any](relationships(10 / 10), 10, "10"),
      Array[Any](relationships(10 / 10), 10, "10")
    ))
  }

  testWithIndex(
    _.supportsValues(EXACT),
    "should use existing values from arguments when available in directed seek"
  ) { index =>
    val propertyType = randomAmong(index.provideValueSupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val lookFor = Seq(randomAmong(relationships), randomAmong(relationships)).map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.relationshipIndexOperator(
        "(x)-[r:R(prop = ???)]->(y)",
        getValue = _ => GetValue,
        paramExpr = Some(varFor("value")),
        argumentIds = Set("value"),
        indexType = index.indexType
      )
      .input(variables = Seq("value"))
      .build()

    val input = inputValues(lookFor.map(p => Array(p.asObject())): _*)

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = lookFor.flatMap { p =>
      relationships
        .filter(propFilter(equalTo(p)))
        .map(nodesAndRelationship)
    }
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  testWithIndex(
    _.supportsValues(EXACT),
    "should use existing values from arguments when available in undirected seek"
  ) { index =>
    val propertyType = randomAmong(index.provideValueSupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val lookFor = Seq(randomAmong(relationships), randomAmong(relationships)).map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.relationshipIndexOperator(
        "(x)-[r:R(prop = ???)]-(y)",
        getValue = _ => GetValue,
        paramExpr = Some(varFor("value")),
        argumentIds = Set("value"),
        indexType = index.indexType
      )
      .input(variables = Seq("value"))
      .build()

    val input = inputValues(lookFor.map(p => Array(p.asObject())): _*)

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = lookFor.flatMap { p =>
      relationships
        .filter(propFilter(equalTo(p)))
        .flatMap(nodesAndRelationshipUndirectional)
    }
    runtimeResult should beColumns("x", "r", "y").withRows(expected, listInAnyOrder = true)
  }

  testWithIndex(
    _.supportsOrderAsc(RANGE),
    "should directed seek relationships of an index with a property in ascending order"
  ) { index =>
    // parallel does not maintain order
    assume(!isParallel)
    val propertyType = randomAmong(index.orderAscSupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("r.prop AS result")
      .relationshipIndexOperator(
        "(x)-[r:R(prop > ???)]->(y)",
        indexOrder = IndexOrderAscending,
        paramExpr = Some(toExpression(someProp)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(greaterThan(someProp)))
      .sorted(propOrdering)
      .map(_.getProperty("prop"))
    runtimeResult should beColumns("result").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderAsc(RANGE),
    "should undirected seek relationships of an index with a property in ascending order"
  ) { index =>
    // parallel does not maintain order
    assume(!isParallel)
    val propertyType = randomAmong(index.orderAscSupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("r.prop AS result")
      .relationshipIndexOperator(
        "(x)-[r:R(prop > ???)]-(y)",
        indexOrder = IndexOrderAscending,
        paramExpr = Some(toExpression(someProp)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .map(r => asValue(r.getProperty("prop")))
      .filter(greaterThan(someProp))
      .sorted(ANY_VALUE_ORDERING)
      .flatMap(p => Seq(p, p))
    runtimeResult should beColumns("result").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderDesc(RANGE),
    "should directed seek relationships of an index with a property in descending order"
  ) { index =>
    // parallel does not maintain order
    assume(!isParallel)

    val propertyType = randomAmong(index.orderDescSupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("r.prop as result")
      .relationshipIndexOperator(
        "(x)-[r:R(prop > ???)]->(y)",
        indexOrder = IndexOrderDescending,
        paramExpr = Some(toExpression(someProp)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .map(r => asValue(r.getProperty("prop")))
      .filter(greaterThan(someProp))
      .sorted(ANY_VALUE_ORDERING.reverse)
    runtimeResult should beColumns("result").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderDesc(RANGE),
    "should undirected seek relationships of an index with a property in descending order"
  ) { index =>
    // parallel does not maintain order
    assume(!isParallel)
    val propertyType = randomAmong(index.orderDescSupport(RANGE))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProp = asValue(randomAmong(relationships).getProperty("prop"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("r.prop AS result")
      .relationshipIndexOperator(
        "(x)-[r:R(prop > ???)]-(y)",
        indexOrder = IndexOrderDescending,
        paramExpr = Some(toExpression(someProp)),
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .map(r => asValue(r.getProperty("prop")))
      .filter(greaterThan(someProp))
      .sorted(ANY_VALUE_ORDERING.reverse)
      .flatMap(p => Seq(p, p))
    runtimeResult should beColumns("result").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderAsc(EXACT),
    "should handle order in multiple directed index seek, ascending"
  ) { index =>
    val propertyType = randomAmong(index.orderAscSupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProps = Seq(randomAmong(relationships), randomAmong(relationships), randomAmong(relationships))
      .map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("r.prop AS prop")
      .relationshipIndexOperator(
        "(x)-[r:R(prop)]->(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(someProps.map(toExpression): _*))),
        indexOrder = IndexOrderAscending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(p => someProps.exists(equalTo(p))))
      .sorted(propOrdering)
      .map(r => r.getProperty("prop"))
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderAsc(EXACT),
    "should handle order in multiple undirected index seek, ascending"
  ) { index =>
    val propertyType = randomAmong(index.orderAscSupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProps = Seq(randomAmong(relationships), randomAmong(relationships), randomAmong(relationships))
      .map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("r.prop AS prop")
      .relationshipIndexOperator(
        "(x)-[r:R(prop)]-(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(someProps.map(toExpression): _*))),
        indexOrder = IndexOrderAscending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(p => someProps.exists(equalTo(p))))
      .sorted(propOrdering)
      .map(_.getProperty("prop"))
      .flatMap(p => Seq(p, p))
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderDesc(EXACT),
    "should handle order in multiple directed index seek, descending"
  ) { index =>
    val propertyType = randomAmong(index.orderDescSupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProps = Seq(randomAmong(relationships), randomAmong(relationships), randomAmong(relationships))
      .map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("r.prop AS prop")
      .relationshipIndexOperator(
        "(x)-[r:R(prop)]->(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(someProps.map(toExpression): _*))),
        indexOrder = IndexOrderDescending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .filter(propFilter(p => someProps.exists(equalTo(p))))
      .sorted(propOrdering.reverse)
      .map(_.getProperty("prop"))
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  testWithIndex(
    _.supportsOrderDesc(EXACT),
    "should handle order in multiple undirected index seek, descending"
  ) { index =>
    val propertyType = randomAmong(index.orderDescSupport(EXACT))
    val relationships = givenGraph(indexedRandomCircleGraph(index.indexType, propertyType))
    val someProps = Seq(randomAmong(relationships), randomAmong(relationships), randomAmong(relationships))
      .map(r => asValue(r.getProperty("prop")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("r.prop AS prop")
      .relationshipIndexOperator(
        "(x)-[r:R(prop)]-(y)",
        customQueryExpression = Some(ManyQueryExpression(listOf(someProps.map(toExpression): _*))),
        indexOrder = IndexOrderDescending,
        indexType = index.indexType
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships
      .map(r => asValue(r.getProperty("prop")))
      .filter(p => someProps.exists(equalTo(p)))
      .sorted(ANY_VALUE_ORDERING.reverse)
      .flatMap(p => Seq(p, p))
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  def indexedRandomCircleGraph(indexType: IndexType, propertyType: ValueType): Seq[Relationship] = {
    indexedCircleGraph(indexType, "R", "prop") {
      case (r, i) if i % 10 == 0 => r.setProperty("prop", randomValue(propertyType).asObject())
    }
  }

  def indexedCircleGraph(
    indexType: IndexType,
    indexedRelType: String,
    indexedProperties: String*
  )(
    propFunction: PartialFunction[(Relationship, Int), Unit]
  ): Seq[Relationship] = {
    relationshipIndex(indexType, indexedRelType, indexedProperties: _*)
    val (_, rels) = circleGraph(sizeHint)
    rels.zipWithIndex.collect {
      case t @ (r, _) if propFunction.isDefinedAt(t) =>
        propFunction.apply(t)
        r
    }
  }

  test("should work with multiple index types") {
    val rels = givenGraph {
      relationshipIndex(IndexType.RANGE, "R", "prop")
      relationshipIndex(IndexType.TEXT, "R", "prop")

      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("prop", i)
        case (r, i) if i % 2 == 1 => r.setProperty("prop", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .union()
      .|.relationshipIndexOperator("(x)-[r:R(prop > 42)]->(y)", indexType = IndexType.RANGE)
      .relationshipIndexOperator("(a)-[r:R(prop CONTAINS '1')]->(b)", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels.filter { r =>
      r.getProperty("prop") match {
        case s: String  => s.contains("1")
        case i: Integer => i > 42
        case _          => false
      }
    }

    runtimeResult should beColumns("r").withRows(singleColumn(expected))
  }

  test("undirected seek should only find loop once") {
    val rel = givenGraph {
      relationshipIndex(IndexType.RANGE, "R", "prop")

      val a = tx.createNode()
      val r = a.createRelationshipTo(a, RelationshipType.withName("R"))
      r.setProperty("prop", 42)
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipIndexOperator("(n)-[r:R(prop = 42)]-(m)", indexType = IndexType.RANGE)
      .build()

    execute(logicalQuery, runtime) should beColumns("r").withSingleRow(rel)
  }

  private def nodesAndRelationship(relationship: Relationship): Array[_] =
    Array(relationship.getStartNode, relationship, relationship.getEndNode)

  private def nodesAndRelationshipUndirectional(relationship: Relationship): Seq[Array[_]] = Seq(
    Array(relationship.getStartNode, relationship, relationship.getEndNode),
    Array(relationship.getEndNode, relationship, relationship.getStartNode)
  )

  private def propFilter(predicate: Value => Boolean): Relationship => Boolean = {
    r => r.hasProperty("prop") && predicate.apply(asValue(r.getProperty("prop")))
  }

  private def propertyFilter(name: String)(predicate: Value => Boolean): Relationship => Boolean = {
    r => r.hasProperty(name) && predicate.apply(asValue(r.getProperty(name)))
  }

  private def equalTo(rhs: Value)(lhs: Value): Boolean = ValueBooleanLogic.equals(lhs, rhs) == BooleanValue.TRUE

  private def greaterThan(rhs: Value)(lhs: Value): Boolean =
    ValueBooleanLogic.greaterThan(lhs, rhs) == BooleanValue.TRUE
  private def lessThan(rhs: Value)(lhs: Value): Boolean = ValueBooleanLogic.lessThan(lhs, rhs) == BooleanValue.TRUE

  def propOrdering: Ordering[Entity] =
    Ordering.by[Entity, AnyValue](e => Values.of(e.getProperty("prop")).asInstanceOf[AnyValue])(ANY_VALUE_ORDERING)

  def propValueOrdering: Ordering[Any] =
    Ordering.by[Any, AnyValue](v => Values.of(v).asInstanceOf[AnyValue])(ANY_VALUE_ORDERING)

}

// not supported by parallel
trait RelationshipLockingUniqueIndexSeekTestBase[CONTEXT <: RuntimeContext] {
  self: RelationshipIndexSeekTestBase[CONTEXT] =>

  test("should grab shared lock when finding a relationship (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) => r.setProperty("prop", i)
      }
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop = $propToFind)]->(y)", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(propToFind)
    runtimeResult should beColumns("r").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should grab shared lock when finding a relationship (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) => r.setProperty("prop", i)
      }
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop = $propToFind)]-(y)", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(propToFind)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected))).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should grab shared lock when finding a relationship (directed, multiple properties)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          r.setProperty("prop1", i)
          r.setProperty("prop2", s"$i")
      }
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop1 = $propToFind, prop2 = '$propToFind')]->(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(propToFind)
    runtimeResult should beColumns("r").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should grab shared lock when finding a relationship (undirected, multiple properties)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          r.setProperty("prop1", i)
          r.setProperty("prop2", s"$i")
      }
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop1 = $propToFind, prop2 = '$propToFind')]-(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(propToFind)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected))).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should grab an exclusive lock when not finding a relationship (directed)") {
    givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          r.setProperty("prop", i)
      }
    }
    val propToFind = sizeHint + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop = $propToFind)]->(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withNoRows().withLocks((EXCLUSIVE, INDEX_ENTRY), (SHARED, RELATIONSHIP_TYPE))
  }

  test("should grab an exclusive lock when not finding a relationship (undirected)") {
    givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          r.setProperty("prop", i)
      }
    }
    val propToFind = sizeHint + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop = $propToFind)]-(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withNoRows().withLocks((EXCLUSIVE, INDEX_ENTRY), (SHARED, RELATIONSHIP_TYPE))
  }

  test("should not grab any lock when readOnly = true (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          r.setProperty("prop", i)
      }
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop = $propToFind)]->(y)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(propToFind)
    runtimeResult should beColumns("r").withSingleRow(expected).withLocks()
  }

  test("should not grab any lock when readOnly = true (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          r.setProperty("prop", i)
      }
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop = $propToFind)]-(y)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(propToFind)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected))).withLocks()
  }

  test("should exact seek relationships of a locking unique index with a property (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          r.setProperty("prop", i)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(20)
    runtimeResult should beColumns("r").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should exact seek relationships of a locking unique index with a property (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          r.setProperty("prop", i)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected))).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should exact seek relationships of a locking composite unique index with properties (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 20, prop2 = '20')]->(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(20)
    runtimeResult should beColumns("r").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should exact seek relationships of a locking composite unique index with properties (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 20, prop2 = '20')]-(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected))).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test(
    "should exact (multiple, but identical) seek relationships of a locking unique index with a property (directed)"
  ) {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop", i)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR 20)]->(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(relationships(20))
    runtimeResult should beColumns("r").withRows(singleColumn(expected)).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test(
    "should exact (multiple, but identical) seek relationships of a locking unique index with a property (undirected)"
  ) {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop", i)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR 20)]-(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected))).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should exact seek relationships of a composite unique index with properties (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 20, prop2 = '20')]->(y)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(20)
    runtimeResult should beColumns("r").withSingleRow(expected)
  }

  test("should exact seek relationships of a composite unique index with properties (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 20, prop2 = '20')]-(y)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected)))
  }

  test("should seek relationships of a composite unique index with properties (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop1 > ${sizeHint / 2}, prop2)]->(y)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter { case (_, i) => i % 10 == 0 && i > sizeHint / 2 }.map(_._1)
    runtimeResult should beColumns("r").withRows(singleColumn(expected))
  }

  test("should seek relationships of a composite unique index with properties (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(s"(x)-[r:R(prop1 > ${sizeHint / 2}, prop2)]-(y)", unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      relationships.zipWithIndex.filter { case (_, i) => i % 10 == 0 && i > sizeHint / 2 }.flatMap(t => Seq(t._1, t._1))
    runtimeResult should beColumns("r").withRows(singleColumn(expected))
  }

  test(
    "should exact (multiple, not identical) seek relationships of a locking unique index with a property (directed)"
  ) {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop", i)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR 30)]->(x)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(relationships(20), relationships(30))
    runtimeResult should beColumns("r").withRows(singleColumn(expected)).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test(
    "should exact (multiple, not identical) seek relationships of a locking unique index with a property (undirected)"
  ) {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop", i)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR 30)]-(x)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(relationships(20), relationships(20), relationships(30), relationships(30))
    runtimeResult should beColumns("r").withRows(singleColumn(expected)).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should support composite index and unique locking (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 10, prop2 = '10')]->(y)", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10)
    runtimeResult should beColumns("r").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should support composite index and unique locking (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 10, prop2 = '10')]-(y)", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected))).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should support composite unique index and unique locking (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 10, prop2 = '10')]->(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10)
    runtimeResult should beColumns("r").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should support composite unique index and unique locking (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop1", "prop2") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop1", i)
            r.setProperty("prop2", i.toString)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 10, prop2 = '10')]-(y)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected))).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should cache properties in locking unique index (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop", i)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .relationshipIndexOperator("(x)-[r:R(prop = 10)]->(y)", _ => GetValue, unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "prop").withSingleRow(relationships(10), 10).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test("should cache properties in locking unique index (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop", i)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .relationshipIndexOperator("(x)-[r:R(prop = 10)]-(y)", _ => GetValue, unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "prop").withRows(Seq.fill(2)(Array[Any](relationships(10), 10))).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, RELATIONSHIP_TYPE)
    )
  }

  test(s"should multi seek nodes of a unique index with locking (directed)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop", i)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(
        "(x)-[r:R(prop)]->(y)",
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
      (SHARED, RELATIONSHIP_TYPE)
    )
    val expected = Seq(Array(relationships.head), Array(relationships(10)), Array(relationships(20)))
    runtimeResult should beColumns("r").withRows(expected).withLocks(expectedLocks: _*)
  }

  test(s"should multi seek nodes of a unique index with locking (undirected)") {
    val relationships = givenGraph {
      indexedCircleGraph(IndexType.RANGE, "R", "prop") {
        case (r, i) =>
          if (i % 10 == 0) {
            r.setProperty("prop", i)
          }
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("true")
      .relationshipIndexOperator(
        "(x)-[r:R(prop)]-(y)",
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
      (SHARED, RELATIONSHIP_TYPE)
    )
    val expected = Seq(
      Array(relationships.head),
      Array(relationships.head),
      Array(relationships(10)),
      Array(relationships(10)),
      Array(relationships(20)),
      Array(relationships(20))
    )
    runtimeResult should beColumns("r").withRows(expected).withLocks(expectedLocks: _*)
  }
}

abstract class ParallelRelationshipIndexSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RelationshipIndexSeekTestBase[CONTEXT](edition, runtime, sizeHint) {
  override def supportedPropertyTypes(): Seq[ValueType] = parallelSupportedTypes
}
