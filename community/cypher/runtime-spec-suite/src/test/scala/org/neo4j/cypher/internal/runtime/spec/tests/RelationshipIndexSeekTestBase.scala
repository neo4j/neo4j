/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.values.storable.ValueType

// Supported by all runtimes
abstract class RelationshipIndexSeekTestBase[CONTEXT <: RuntimeContext](
                                                                         edition: Edition[CONTEXT],
                                                                         runtime: CypherRuntime[CONTEXT],
                                                                         val sizeHint: Int
                                                                       )
  extends RuntimeTestSuite[CONTEXT](
    runtime = runtime,
    edition = edition
  )
  with RandomValuesTestSupport
{

  for (indexProvider <- Seq(GenericNativeIndexProvider.DESCRIPTOR.name())) {

    test(s"should exact (single) directed relationship seek of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())
      val lookFor = randomAmong(relationships).getProperty("prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = ???)]->(y)", paramExpr = Some(toExpression(lookFor)))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"should exact (single) undirected relationship seek of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())
      val lookFor = randomAmong(relationships).getProperty("prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = ???)]-(y)", paramExpr = Some(toExpression(lookFor)))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"should exact (single) directed seek nodes of an index with a property with multiple matches (${indexProvider})") {
      val propertyValues = randomValues(5, randomPropertyType())
      val relationships = given {
        defaultIndexedCircleGraph { case (r, _) => r.setProperty("prop", randomAmong(propertyValues).asObject()) }
      }

      val lookFor = propertyValues.head.asObject()

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = ???)]->(y)", paramExpr = Some(toExpression(lookFor)))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"should exact (single) undirected seek nodes of an index with a property with multiple matches (${indexProvider})") {
      val propertyValues = randomValues(5, randomPropertyType())
      val relationships = given {
        defaultIndexedCircleGraph { case (r, _) => r.setProperty("prop", randomAmong(propertyValues).asObject()) }
      }

      val lookFor = propertyValues.head.asObject()

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = ???)]-(y)", paramExpr = Some(toExpression(lookFor)))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"exact single directed seek should handle null (${indexProvider})") {
      given {
        defaultIndexedCircleGraph { case (r, i) if i % 10 == 0 => r.setProperty("prop", randomPropertyValue().asObject()) }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = ???)]->(y)", paramExpr = Some(nullLiteral))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"exact single undirected seek should handle null (${indexProvider})") {
      given {
        defaultIndexedCircleGraph { case (r, i) if i % 10 == 0 => r.setProperty("prop", randomPropertyValue().asObject()) }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = ???)]-(y)", paramExpr = Some(nullLiteral))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"should exact (multiple) directed seek nodes of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())

      val lookFor = Seq(
        randomAmong(relationships).getProperty("prop"),
        randomAmong(relationships).getProperty("prop")
      )

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop)]->(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor(0)), toExpression(lookFor(1)))))
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](lookFor.contains)).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"should exact (multiple) undirected seek nodes of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())

      val lookFor = Seq(
        randomAmong(relationships).getProperty("prop"),
        randomAmong(relationships).getProperty("prop")
      )

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop)]-(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor(0)), toExpression(lookFor(1)))))
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](lookFor.contains)).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"should handle null in exact multiple directed seek (${indexProvider})") {
      given {
        defaultIndexedCircleGraph { case (r, i) if i % 10 == 0 => r.setProperty("prop", randomPropertyValue().asObject()) }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop IN ???)]->(y)", paramExpr = Some(nullLiteral))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"should handle null in exact multiple undirected seek (${indexProvider})") {
      given {
        defaultIndexedCircleGraph { case (r, i) if i % 10 == 0 => r.setProperty("prop", randomPropertyValue().asObject()) }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop IN ???)]-(y)", paramExpr = Some(nullLiteral))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"should exact (multiple, but empty) directed seek relationships of an index with a property (${indexProvider})") {
      given {
        defaultIndexedCircleGraph { case (r, i) if i % 10 == 0 => r.setProperty("prop", randomPropertyValue().asObject()) }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop)]->(y)", customQueryExpression = Some(ManyQueryExpression(listOf())))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"should exact (multiple, but empty) undirected seek relationships of an index with a property (${indexProvider})") {
      given {
        defaultIndexedCircleGraph { case (r, i) if i % 10 == 0 => r.setProperty("prop", i) }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop)]-(y)", customQueryExpression = Some(ManyQueryExpression(listOf())))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"should exact (multiple, with null) directed seek relationships of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())

      val lookFor = randomAmong(relationships).getProperty("prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop)]->(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), nullLiteral)))
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"should exact (multiple, with null) undirected seek relationships of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())
      val lookFor = randomAmong(relationships).getProperty("prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop)]-(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), nullLiteral)))
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"should support directed seek on composite index with random type (${indexProvider})") {
      val propertyType1 = randomPropertyType()
      val propertyType2 = randomPropertyType()
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", randomValue(propertyType1).asObject())
            r.setProperty("prop2", randomValue(propertyType2).asObject())
        }
      }

      val lookForRelationship = randomAmong(relationships)
      val lookFor1 = lookForRelationship.getProperty("prop")
      val lookFor2 = lookForRelationship.getProperty("prop2")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop, prop2)]->(y)",
          customQueryExpression = Some(CompositeQueryExpression(Seq(
            SingleQueryExpression(toExpression(lookFor1)),
            SingleQueryExpression(toExpression(lookFor2))
          )))
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect {
        case r if r.getProperty("prop") == lookFor1 && r.getProperty("prop2") == lookFor2 => Array(r.getStartNode, r, r.getEndNode)
      }
      runtimeResult should beColumns("x", "r", "y").withRows(inAnyOrder(expected))
    }

    test(s"should exact (multiple, but identical) directed seek relationships of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())
      val lookFor = randomAmong(relationships).getProperty("prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop)]->(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), toExpression(lookFor))))
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should exact (multiple, but identical) undirected seek relationships of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())
      val lookFor = randomAmong(relationships).getProperty("prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(
          indexSeekString = "(x)-[r:R(prop)]-(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(toExpression(lookFor), toExpression(lookFor))))
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    // RANGE queries
    test(s"should directed seek relationships of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Int](_ > sizeHint / 2)).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should undirected seek relationships of an index with a property (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Int](_ > sizeHint / 2)).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should directed seek relationships with multiple less than bounds (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 2)]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Int](_ < 1)).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should undirected seek relationships with multiple less than bounds (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 2)]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Int](_ < 1)).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should directed seek relationships with multiple less than bounds with different types (${indexProvider})") {
      given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 'foo')]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"should undirected seek relationships with multiple less than bounds with different types (${indexProvider})") {
      given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 'foo')]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }


    test(s"should support directed seek on composite index (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", i)
            r.setProperty("prop2", i.toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = nodesAndRelationship(relationships(10 / 10))
      runtimeResult should beColumns("x", "r", "y").withSingleRow(expected:_*)
    }

    test(s"should support undirected seek on composite index (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", i)
            r.setProperty("prop2", i.toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = nodesAndRelationshipUndirectional(relationships(10 / 10))
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support directed seek on composite index (multiple results) (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 2 == 0 =>
            r.setProperty("prop", i % 5)
            r.setProperty("prop2", i % 3)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 0, prop2 = 0)]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .collect { case r if r.getProperty("prop") == 0 && r.getProperty("prop2") == 0 => nodesAndRelationship(r) }
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support undirected seek on composite index (multiple results) (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 2 == 0 =>
            r.setProperty("prop", (i % 5).toString)
            r.setProperty("prop2", (i % 3).toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = '0', prop2 = '0')]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .filter(r => r.getProperty("prop") == "0" && r.getProperty("prop2") == "0")
        .flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support directed seek on composite index (multiple values) (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", i)
            r.setProperty("prop2", i.toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 10 OR 20, prop2 = '10' OR '30')]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships(10 / 10)
      runtimeResult should beColumns("x", "r", "y").withSingleRow(expected.getStartNode, expected, expected.getEndNode)
    }

    test(s"should support undirected seek on composite index (multiple values) (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", i)
            r.setProperty("prop2", i.toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 10 OR 20, prop2 = '10' OR '30')]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = nodesAndRelationshipUndirectional(relationships(10 / 10))
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support directed composite index seek with equality and existence check (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
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
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2)]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .filter(n => n.hasProperty("prop2") && n.getProperty("prop").asInstanceOf[Int] == 10)
        .map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support undirected composite index seek with equality and existence check (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
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
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2)]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .filter(n => n.hasProperty("prop2") && n.getProperty("prop").asInstanceOf[Int] == 10)
        .flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support directed composite index seek with equality and range check (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) =>
            r.setProperty("prop", i % 20)
            r.setProperty("prop2", i)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 > 10)]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .filter(n => n.getProperty("prop") == 10 && n.getProperty("prop2").asInstanceOf[Int] > 10)
        .map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support undirected composite index seek with equality and range check (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) =>
            r.setProperty("prop", i % 20)
            r.setProperty("prop2", i)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 > 10)]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .filter(n => n.getProperty("prop") == 10 && n.getProperty("prop2").asInstanceOf[Int] > 10)
        .flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support directed seek on composite index with range check and existence check (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
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
        .relationshipIndexOperator("(x)-[r:R(prop > 10, prop2)]->(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .filter(n => n.getProperty("prop").asInstanceOf[Int] > 10 && n.hasProperty("prop2"))
        .map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support undirected seek on composite index with range check and existence check (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
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
        .relationshipIndexOperator("(x)-[r:R(prop > 10, prop2)]-(y)")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .filter(n => n.getProperty("prop").asInstanceOf[Int] > 10 && n.hasProperty("prop2"))
        .flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should support null in directed seek on composite index (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", i)
            r.setProperty("prop2", i.toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = ???)]->(y)", paramExpr = Some(nullLiteral))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = nodesAndRelationship(relationships(10 / 10))
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"should support null in undirected seek on composite index (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", i)
            r.setProperty("prop2", i.toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = ???)]-(y)", paramExpr = Some(nullLiteral))
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = nodesAndRelationship(relationships(10 / 10))
      runtimeResult should beColumns("x", "r", "y").withNoRows()
    }

    test(s"directed exact seek should cache properties (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())
      val lookFor = randomAmong(relationships).getProperty("prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "prop")
        .projection("cacheR[r.prop] AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop = ???)]->(y)", paramExpr = Some(toExpression(lookFor)), getValue = _ => GetValue)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Any](_ == lookFor)).map(r =>  Array(r, lookFor))
      runtimeResult should beColumns("r", "prop").withRows(inAnyOrder(expected))
    }

    test(s"undirected exact seek should cache properties (${indexProvider})") {
      val relationships = given(defaultIndexedRandomCircleGraph())
      val lookFor = randomAmong(relationships).getProperty("prop")

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "prop")
        .projection("cacheR[r.prop] AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop = ???)]-(y)", paramExpr = Some(toExpression(lookFor)), getValue = _ => GetValue)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.flatMap(r => Seq(r,r)).filter(propFilter[Any](_ == lookFor)).map(r =>  Array(r, lookFor))
      runtimeResult should beColumns("r", "prop").withRows(inAnyOrder(expected))
    }

    test(s"directed seek should cache properties (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "prop")
        .projection("cacheR[r.prop] AS prop")
        .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)", _ => GetValue)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect {
        case r if r.getProperty("prop").asInstanceOf[Int] > sizeHint / 2 => Array(r, r.getProperty("prop"))
      }
      runtimeResult should beColumns("r", "prop").withRows(expected)
    }

    test(s"undirected seek should cache properties (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "prop")
        .projection("cacheR[r.prop] AS prop")
        .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)", _ => GetValue)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect {
        case r if r.getProperty("prop").asInstanceOf[Int] > sizeHint / 2 => Seq.fill(2)(Array(r, r.getProperty("prop")))}.flatten
      runtimeResult should beColumns("r", "prop").withRows(expected)
    }

    test(s"directed composite seek should cache properties (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
          case (r, i) if i % 10 == 0 =>
            r.setProperty("prop", i)
            r.setProperty("prop2", i.toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "prop", "prop2")
        .projection("cacheR[r.prop] AS prop", "cacheR[r.prop2] AS prop2")
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]->(y)", _ => GetValue)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("r", "prop", "prop2").withSingleRow(relationships(10 / 10), 10, "10")
    }

    test(s"undirected composite seek should cache properties (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop", "prop2") {
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
        .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]-(y)", _ => GetValue)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      runtimeResult should beColumns("r", "prop", "prop2").withRows(Seq(Array(relationships(10 / 10), 10, "10"), Array(relationships(10 / 10), 10, "10")))
    }

    test(s"should use existing values from arguments when available in directed seek (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .apply()
        .|.relationshipIndexOperator("(x)-[r:R(prop = ???)]->(y)", _ => GetValue, paramExpr = Some(varFor("value")), argumentIds = Set("value"))
        .input(variables = Seq("value"))
        .build()

      val input = inputValues(Array(20), Array(50))

      val runtimeResult = execute(logicalQuery, runtime, input)

      // then
      val expected = Seq(relationships(20 / 10), relationships(50 / 10)).map(nodesAndRelationship)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should use existing values from arguments when available in undirected seek (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "r", "y")
        .apply()
        .|.relationshipIndexOperator("(x)-[r:R(prop = ???)]-(y)", _ => GetValue, paramExpr = Some(varFor("value")), argumentIds = Set("value"))
        .input(variables = Seq("value"))
        .build()

      val input = inputValues(Array(20), Array(50))

      val runtimeResult = execute(logicalQuery, runtime, input)

      // then
      val expected = Seq(relationships(20 / 10), relationships(50 / 10)).flatMap(nodesAndRelationshipUndirectional)
      runtimeResult should beColumns("x", "r", "y").withRows(expected)
    }

    test(s"should directed seek relationships of an index with a property in ascending order (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)", indexOrder = IndexOrderAscending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Int](_ > sizeHint / 2))
      runtimeResult should beColumns("r").withRows(singleColumnInOrder(expected))
    }

    test(s"should undirected seek relationships of an index with a property in ascending order (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)", indexOrder = IndexOrderAscending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Int](_ > sizeHint / 2)).flatMap(r => Seq(r, r))
      runtimeResult should beColumns("r").withRows(singleColumnInOrder(expected))
    }


    test(s"should directed seek relationships of an index with a property in descending order (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())
      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)", indexOrder = IndexOrderDescending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Int](_ > sizeHint / 2)).reverse
      runtimeResult should beColumns("r").withRows(singleColumnInOrder(expected))
    }

    test(s"should undirected seek relationships of an index with a property in descending order (${indexProvider})") {
      val relationships = given(defaultIndexedIntCircleGraph())
      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)", indexOrder = IndexOrderDescending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.filter(propFilter[Int](_ > sizeHint / 2)).flatMap(r => Seq(r, r)).reverse
      runtimeResult should beColumns("r").withRows(singleColumnInOrder(expected))
    }

    test(s"should handle order in multiple directed index seek, ascending int (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop") {
          case (r, i) => r.setProperty("prop", i % 10)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("r.prop AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop)]->(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(literalInt(7), literalInt(2), literalInt(3)))),
          indexOrder = IndexOrderAscending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect(collectProp[Int](Set(7, 2, 3).contains)).sorted
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"should handle order in multiple directed index seek, ascending string (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop") {
          case (r, i) => r.setProperty("prop", (i % 10).toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("r.prop AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop)]->(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(literalString("7"), literalString("2"), literalString("3")))),
          indexOrder = IndexOrderAscending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect(collectProp[String](Set("7", "2", "3").contains)).sorted
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"should handle order in multiple undirected index seek, ascending int (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop") {
          case (r, i) => r.setProperty("prop", i % 10)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("r.prop AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop)]-(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(literalInt(7), literalInt(2), literalInt(3)))),
          indexOrder = IndexOrderAscending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect(collectProp[Int](Set(7, 2, 3).contains)).sorted.flatMap(r => Seq(r, r))
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"should handle order in multiple undirected index seek, ascending string (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop") {
          case (r, i) => r.setProperty("prop", (i % 10).toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("r.prop AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop)]-(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(literalString("7"), literalString("2"), literalString("3")))),
          indexOrder = IndexOrderAscending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect(collectProp[String](Set("7", "2", "3").contains)).sorted.flatMap(p => Seq(p, p))
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"should handle order in multiple directed index seek, descending int (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop") {
          case (r, i) => r.setProperty("prop", i % 10)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("r.prop AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop)]->(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(literalInt(7), literalInt(2), literalInt(3)))),
          indexOrder = IndexOrderDescending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect(collectProp[Int](Set(7, 2, 3).contains)).sorted(Ordering.Int.reverse)
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"should handle order in multiple directed index seek, descending string (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop") {
          case (r, i) => r.setProperty("prop", (i % 10).toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("r.prop AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop)]->(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(literalString("7"), literalString("2"), literalString("3")))),
          indexOrder = IndexOrderDescending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships.collect(collectProp[String](Set("7", "2", "3").contains)).sorted(Ordering.String.reverse)
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"should handle order in multiple undirected index seek, descending int (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop") {
          case (r, i) => r.setProperty("prop", i % 10)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("r.prop AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop)]-(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(literalInt(7), literalInt(2), literalInt(3)))),
          indexOrder = IndexOrderDescending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .collect(collectProp[Int](Set(7, 2, 3).contains))
        .sorted(Ordering.Int.reverse)
        .flatMap(r => Seq(r, r))
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    test(s"should handle order in multiple undirected index seek, descending string (${indexProvider})") {
      val relationships = given {
        indexedCircleGraph("R", "prop") {
          case (r, i) => r.setProperty("prop", (i % 10).toString)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("r.prop AS prop")
        .relationshipIndexOperator("(x)-[r:R(prop)]-(y)",
          customQueryExpression = Some(ManyQueryExpression(listOf(literalString("7"), literalString("2"), literalString("3")))),
          indexOrder = IndexOrderDescending)
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val expected = relationships
        .collect(collectProp[String](Set("7", "2", "3").contains))
        .sorted(Ordering.String.reverse)
        .flatMap(r => Seq(r, r))
      runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
    }

    def defaultIndexedRandomCircleGraph(): Seq[Relationship] = {
      val propertyType = randomPropertyType()
      defaultIndexedCircleGraph {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", randomValue(propertyType).asObject())
      }
    }

    def defaultIndexedIntCircleGraph(): Seq[Relationship] = {
      defaultIndexedCircleGraph { case (r, i) if i % 10 == 0 => r.setProperty("prop", i) }
    }

    def defaultIndexedCircleGraph(propFunction: PartialFunction[(Relationship, Int), Unit]): Seq[Relationship] = {
      indexedCircleGraph("R", "prop")(propFunction)
    }

    def indexedCircleGraph(indexedRelType: String, indexedProperties: String*)(propFunction: PartialFunction[(Relationship, Int), Unit]): Seq[Relationship] = {
      relationshipIndexWithProvider(indexProvider, indexedRelType, indexedProperties:_*)
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.collect {
        case t@(r, _) if propFunction.isDefinedAt(t) =>
          propFunction.apply(t)
          r
      }
    }
  }

  test("should work with multiple index types") {
    val rels = given {
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
        case s: String => s.contains("1")
        case i: Integer => i > 42
        case _ => false
      }
    }

    runtimeResult should beColumns("r").withRows(singleColumn(expected))
  }

  private def propFilter[T](predicate: T => Boolean): Relationship => Boolean = {
    r => predicate.apply(r.getProperty("prop").asInstanceOf[T])
  }

  private def collectProp[T](predicate: T => Boolean): PartialFunction[Relationship, T] = {
    case r if predicate(r.getProperty("prop").asInstanceOf[T]) => r.getProperty("prop").asInstanceOf[T]
  }

  private def nodesAndRelationship(relationship: Relationship): Array[_] = Array(relationship.getStartNode, relationship, relationship.getEndNode)

  private def nodesAndRelationshipUndirectional(relationship: Relationship): Seq[Array[_]] = Seq(
    Array(relationship.getStartNode, relationship, relationship.getEndNode),
    Array(relationship.getEndNode, relationship, relationship.getStartNode)
  )
}

abstract class ParallelRelationshipIndexSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RelationshipIndexSeekTestBase[CONTEXT](edition, runtime, sizeHint) {
  private val supportedPropertyTypes: Array[ValueType] = {
    stringValueTypes ++ numericValueTypes :+ ValueType.BOOLEAN
  }

  // Parallel does not support functions at the moment so we can only use properties that have literals
  override def propertyValueTypes: Array[ValueType] = supportedPropertyTypes
}
