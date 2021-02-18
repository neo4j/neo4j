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
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

// Supported by all runtimes
abstract class RelationshipIndexSeekTestBase[CONTEXT <: RuntimeContext](
                                                                         edition: Edition[CONTEXT],
                                                                         runtime: CypherRuntime[CONTEXT],
                                                                         val sizeHint: Int
                                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should exact (single) directed relationship seek of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val r = relationships(20)
    runtimeResult should beColumns("x", "r", "y").withSingleRow(r.getStartNode, r, r.getEndNode)
  }

  test("should exact (single) undirected relationship seek of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val r = relationships(20)
    runtimeResult should beColumns("x", "r", "y").withRows(Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode)))
  }

  test("should exact (single) directed seek nodes of an index with a property with multiple matches") {
    val numMatches = sizeHint / 5
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i < numMatches => r.setProperty("prop", "foo")
        case (r, _) => r.setProperty("prop", "bar")
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 'foo')]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withRows(relationships.take(numMatches).map(r => Array(r.getStartNode, r, r.getEndNode)))
  }

  test("should exact (single) udirected seek nodes of an index with a property with multiple matches") {
    val numMatches = sizeHint / 5
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i < numMatches => r.setProperty("prop", "foo")
        case (r, _) => r.setProperty("prop", "bar")
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 'foo')]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withRows(relationships
      .take(numMatches)
      .flatMap(r =>
        Seq(
          Array(r.getStartNode, r, r.getEndNode),
          Array(r.getEndNode, r, r.getStartNode))
      )
    )
  }

  test("exact single directed seek should handle null") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
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

  test("exact single undirected seek should handle null") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
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

  test("should exact (multiple) directed seek nodes of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR 30)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val r1 = relationships(20)
    val r2 = relationships(30)
    // then
    val expected = Seq(
      Array(r1.getStartNode, r1, r1.getEndNode),
      Array(r2.getStartNode, r2, r2.getEndNode)
    )
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should exact (multiple) undirected seek nodes of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR 30)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val r1 = relationships(20)
    val r2 = relationships(30)
    // then
    val expected = Seq(
      Array(r1.getStartNode, r1, r1.getEndNode),
      Array(r1.getEndNode, r1, r1.getStartNode),
      Array(r2.getStartNode, r2, r2.getEndNode),
      Array(r2.getEndNode, r2, r2.getStartNode)
    )
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should handle null in exact multiple directed seek") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
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

  test("should handle null in exact multiple undirected seek") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
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

  test("should exact (multiple, but empty) directed seek relationships of an index with a property") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
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

  test("should exact (multiple, but empty) undirected seek relationships of an index with a property") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
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

  test("should exact (multiple, with null) directed seek relationships of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR ???)]->(y)", paramExpr = Some(nullLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val rel = relationships(20)
    runtimeResult should beColumns("x", "r", "y").withSingleRow(rel.getStartNode, rel, rel.getEndNode)
  }

  test("should exact (multiple, with null) undirected seek relationships of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR ???)]-(y)", paramExpr = Some(nullLiteral))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val rel = relationships(20)
    runtimeResult should beColumns("x", "r", "y").withRows(Seq(Array(rel.getStartNode, rel, rel.getEndNode), Array(rel.getEndNode, rel, rel.getStartNode)))
  }

  test("should exact (multiple, but identical) directed seek relationships of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR 20)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val relationship = relationships(20)
    // then
    val expected = Seq(Array(relationship.getStartNode, relationship, relationship.getEndNode))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should exact (multiple, but identical) undirected seek relationships of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 20 OR 20)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val relationship = relationships(20)
    // then
    val expected = Seq(
      Array(relationship.getStartNode, relationship, relationship.getEndNode),
      Array(relationship.getEndNode, relationship, relationship.getStartNode))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  // RANGE queries
  test("should directed seek relationships of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map {
      case (r, _) => Array(r.getStartNode, r, r.getEndNode)
    }

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should undirected seek relationships of an index with a property") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.flatMap {
      case (r, _) => Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode))
    }
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should directed seek relationships with multiple less than bounds") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 2)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter{ case (_, i) => i < 1 }.map {
      case (r, _) => Array(r.getStartNode, r, r.getEndNode)
    }
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should undirected seek relationships with multiple less than bounds") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 2)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter{ case (_, i) => i < 1 }.flatMap {
      case (r, _) => Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode))
    }
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should directed seek relationships with multiple less than bounds with different types") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 'foo')]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  test("should undirected seek relationships with multiple less than bounds with different types") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator(s"(x)-[r:R(1 > prop < 'foo')]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }


  test("should support directed seek on composite index") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => {
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        }
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10)
    runtimeResult should beColumns("x", "r", "y").withSingleRow(expected.getStartNode, expected, expected.getEndNode)
  }

  test("should support undirected seek on composite index") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => {
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        }
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10)
    runtimeResult should beColumns("x", "r", "y").withRows(Seq(
      Array(expected.getStartNode, expected, expected.getEndNode),
      Array(expected.getEndNode, expected, expected.getStartNode))
    )
  }

  test("should support directed seek on composite index (multiple results)") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => {
          r.setProperty("prop", i % 5)
          r.setProperty("prop2", i % 3)
        }
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 0, prop2 = 0)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.collect { case (r, i) if i % 2 == 0 && i % 5 == 0 && i % 3 == 0 => r }
      .map(r => Array(r.getStartNode, r, r.getEndNode))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should support undirected seek on composite index (multiple results)") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => {
          r.setProperty("prop", i % 5)
          r.setProperty("prop2", i % 3)
        }
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 0, prop2 = 0)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.collect { case (r, i) if i % 2 == 0 && i % 5 == 0 && i % 3 == 0 => r }
      .flatMap(r => Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode)))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should support directed seek on composite index (multiple values)") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => {
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        }
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10 OR 20, prop2 = '10' OR '30')]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10)
    runtimeResult should beColumns("x", "r", "y").withSingleRow(expected.getStartNode, expected, expected.getEndNode)
  }

  test("should support undirected seek on composite index (multiple values)") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => {
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        }
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10 OR 20, prop2 = '10' OR '30')]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships(10)
    runtimeResult should beColumns("x", "r", "y").withRows(Seq(
      Array(expected.getStartNode, expected, expected.getEndNode),
      Array(expected.getEndNode, expected, expected.getStartNode)
    ))
  }

  test("should support directed composite index seek with equality and existence check") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 3 == 0 =>
          r.setProperty("prop", i % 20)
          r.setProperty("prop2", i.toString)
        case (r, i) =>
          r.setProperty("prop", i % 20)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(n => n.hasProperty("prop2") && n.getProperty("prop").asInstanceOf[Int] == 10)
      .map(r => Array(r.getStartNode, r, r.getEndNode))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should support undirected composite index seek with equality and existence check") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 3 == 0 =>
          r.setProperty("prop", i % 20)
          r.setProperty("prop2", i.toString)
        case (r, i) =>
          r.setProperty("prop", i % 20)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(n => n.hasProperty("prop2") && n.getProperty("prop").asInstanceOf[Int] == 10)
      .flatMap(r => Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode)))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should support directed composite index seek with equality and range check") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("prop", i % 20)
          r.setProperty("prop2", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 > 10)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(n => n.getProperty("prop").asInstanceOf[Int] == 10 && n.getProperty("prop2").asInstanceOf[Int] > 10)
      .map(r => Array(r.getStartNode, r, r.getEndNode))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should support undirected composite index seek with equality and range check") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("prop", i % 20)
          r.setProperty("prop2", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 > 10)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(n => n.getProperty("prop").asInstanceOf[Int] == 10 && n.getProperty("prop2").asInstanceOf[Int] > 10)
      .flatMap(r => Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode)))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should support directed seek on composite index with range check and existence check") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 3 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        case (r, i) =>
          r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop > 10, prop2)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(n => n.getProperty("prop").asInstanceOf[Int] > 10 && n.hasProperty("prop2"))
      .map(r => Array(r.getStartNode, r, r.getEndNode))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should support undirected seek on composite index with range check and existence check") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 3 == 0 =>
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        case (r, i) =>
          r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .relationshipIndexOperator("(x)-[r:R(prop > 10, prop2)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.filter(n => n.getProperty("prop").asInstanceOf[Int] > 10 && n.hasProperty("prop2"))
      .flatMap(r => Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode)))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("directed seek should cache properties") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.collect{ case (n, i) if (i % 10) == 0 && i > sizeHint / 2 => Array(n, i)}
    runtimeResult should beColumns("r", "prop").withRows(expected)
  }

  test("undirected seek should cache properties") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.collect{ case (n, i) if (i % 10) == 0 && i > sizeHint / 2 => Seq.fill(2)(Array(n, i))}.flatten
    runtimeResult should beColumns("r", "prop").withRows(expected)
  }

  test("directed composite seek should cache properties") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => {
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        }
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop", "prop2")
      .projection("cacheR[r.prop] AS prop", "cacheR[r.prop2] AS prop2")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]->(y)", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "prop", "prop2").withSingleRow(relationships(10), 10, "10")
  }

  test("undirected composite seek should cache properties") {
    val relationships = given {
      relationshipIndex("R", "prop", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => {
          r.setProperty("prop", i)
          r.setProperty("prop2", i.toString)
        }
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop", "prop2")
      .projection("cacheR[r.prop] AS prop", "cacheR[r.prop2] AS prop2")
      .relationshipIndexOperator("(x)-[r:R(prop = 10, prop2 = '10')]-(y)", GetValue)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "prop", "prop2").withRows(Seq(Array(relationships(10), 10, "10"), Array(relationships(10), 10, "10")))
  }

  test("should use existing values from arguments when available in directed seek") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(prop = ???)]->(y)", GetValue, paramExpr = Some(varFor("value")), argumentIds = Set("value"))
      .input(variables = Seq("value"))
      .build()

    val input = inputValues(Array(20), Array(50))

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = Seq(relationships(20), relationships(50)).map(r => Array(r.getStartNode, r, r.getEndNode))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should use existing values from arguments when available in undirected seek") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(prop = ???)]-(y)", GetValue, paramExpr = Some(varFor("value")), argumentIds = Set("value"))
      .input(variables = Seq("value"))
      .build()

    val input = inputValues(Array(20), Array(50))

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = Seq(relationships(20), relationships(50)).flatMap(r => Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode)))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should directed seek relationships of an index with a property in ascending order") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)", indexOrder = IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1)
    runtimeResult should beColumns("r").withRows(singleColumnInOrder(expected))
  }

  test("should undirected seek relationships of an index with a property in ascending order") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)", indexOrder = IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.flatMap(t => Seq.fill(2)(t._1))
    runtimeResult should beColumns("r").withRows(singleColumnInOrder(expected))
  }


  test("should directed seek relationships of an index with a property in descending order") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)", indexOrder = IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.map(_._1).reverse
    runtimeResult should beColumns("r").withRows(singleColumnInOrder(expected))
  }

  test("should undirected seek relationships of an index with a property in descending order") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _ =>
      }
      rels
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)", indexOrder = IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.filter{ case (_, i) => i % 10 == 0 && i > sizeHint / 2}.flatMap(t => Seq.fill(2)(t._1)).reverse
    runtimeResult should beColumns("r").withRows(singleColumnInOrder(expected))
  }

  test("should handle order in multiple directed index seek, ascending") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 10)
      }
      rels
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
    val keys = Set(7, 2, 3)
    val expected = relationships.collect {
      case r if keys(r.getProperty("prop").asInstanceOf[Int]) => r.getProperty("prop").asInstanceOf[Int]
    }.sorted
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  test("should handle order in multiple undirected in˚dex seek, ascending") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 10)
      }
      rels
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
    val keys = Set(7, 2, 3)
    val expected = relationships.collect {
      case r if keys(r.getProperty("prop").asInstanceOf[Int]) => r.getProperty("prop").asInstanceOf[Int]
    }.sorted.flatMap(r => Seq(r, r))
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  test("should handle order in multiple directed index seek, descending") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 10)
      }
      rels
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
    val keys = Set(7, 2, 3)
    val expected = relationships.collect {
      case r if keys(r.getProperty("prop").asInstanceOf[Int]) => r.getProperty("prop").asInstanceOf[Int]
    }.sorted(Ordering.Int.reverse)
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }

  test("should handle order in multiple undirected index seek, descending") {
    val relationships = given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 10)
      }
      rels
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
    val keys = Set(7, 2, 3)
    val expected = relationships.collect {
      case r if keys(r.getProperty("prop").asInstanceOf[Int]) => r.getProperty("prop").asInstanceOf[Int]
    }.sorted(Ordering.Int.reverse).flatMap(r => Seq(r, r))
    runtimeResult should beColumns("prop").withRows(singleColumnInOrder(expected))
  }
}
