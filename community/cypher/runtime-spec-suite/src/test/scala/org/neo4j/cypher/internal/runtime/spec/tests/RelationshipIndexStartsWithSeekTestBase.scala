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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexType

abstract class RelationshipIndexStartsWithSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should be case sensitive for STARTS WITH with indexes directed seek") {
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("text", "CASE")
        case (r, _)               => r.setProperty("text", "case")
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("r.text AS text")
      .relationshipIndexOperator("(x)-[r:R(text STARTS WITH 'ca')]->(y)", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = List.fill(sizeHint / 2)("case")
    runtimeResult should beColumns("text").withRows(singleColumn(expected))
  }

  test("should be case sensitive for STARTS WITH with indexes undirected seek") {
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("text", "CASE")
        case (r, _)               => r.setProperty("text", "case")
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("r.text AS text")
      .relationshipIndexOperator("(x)-[r:R(text STARTS WITH 'ca')]-(y)", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = List.fill(sizeHint)("case")
    runtimeResult should beColumns("text").withRows(singleColumn(expected))
  }

  test("should handle null input directed") {
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("text", "CASE")
        case (r, _)               => r.setProperty("text", "case")
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("r.text AS text")
      .relationshipIndexOperator(
        "(x)-[r:R(text STARTS WITH ???)]->(y)",
        paramExpr = Some(nullLiteral),
        indexType = IndexType.TEXT
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("text").withNoRows()
  }

  test("should handle null input undirected") {
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("text", "CASE")
        case (r, _)               => r.setProperty("text", "case")
      }
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("r.text AS text")
      .relationshipIndexOperator(
        "(x)-[r:R(text STARTS WITH ???)]-(y)",
        paramExpr = Some(nullLiteral),
        indexType = IndexType.TEXT
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("text").withNoRows()
  }

  test("should handle non-text input directed") {
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("text", "CASE")
        case (r, _)               => r.setProperty("text", "case")
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("r.text AS text")
      .relationshipIndexOperator("(x)-[r:R(text STARTS WITH 1337)]->(y)", indexType = IndexType.TEXT)
      .build()

    // then
    execute(logicalQuery, runtime) should beColumns("text").withNoRows()
  }

  test("should handle non-text input undirected") {
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("text", "CASE")
        case (r, _)               => r.setProperty("text", "case")
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("r.text AS text")
      .relationshipIndexOperator("(x)-[r:R(text STARTS WITH 1337)]-(y)", indexType = IndexType.TEXT)
      .build()

    // then
    execute(logicalQuery, runtime) should beColumns("text").withNoRows()
  }

  // We have no query that supports this at the moment
  ignore("should cache properties directed") {
    val relationships = given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("text", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "text")
      .projection("cacheR[r.text] AS text")
      .relationshipIndexOperator("(x)-[r:R(text STARTS WITH '1')]->(y)", _ => GetValue, indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      relationships.zipWithIndex.collect { case (r, i) if i.toString.startsWith("1") => Array[Object](r, i.toString) }
    runtimeResult should beColumns("r", "text").withRows(expected)
  }

  // We have no index that supports this query at the moment
  ignore("should cache properties undirected") {
    val relationships = given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("text", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "text")
      .projection("cacheR[r.text] AS text")
      .relationshipIndexOperator("(x)-[r:R(text STARTS WITH '1')]-(y)", _ => GetValue, indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relationships.zipWithIndex.flatMap {
      case (r, i) if i.toString.startsWith("1") =>
        Seq(
          Array[Object](r, i.toString),
          Array[Object](r, i.toString)
        )
      case _ => Seq.empty
    }
    runtimeResult should beColumns("r", "text").withRows(expected)
  }

  test("undirected scans only find loop once") {
    val rel = given {
      relationshipIndex(IndexType.TEXT, "R", "text")

      val a = tx.createNode()
      val r = a.createRelationshipTo(a, RelationshipType.withName("R"))
      r.setProperty("text", "value")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipIndexOperator("(n)-[r:R(text STARTS WITH 'val')]-(m)", indexType = IndexType.TEXT)
      .build(readOnly = false)

    execute(logicalQuery, runtime) should beColumns("r").withSingleRow(rel)
  }
}
