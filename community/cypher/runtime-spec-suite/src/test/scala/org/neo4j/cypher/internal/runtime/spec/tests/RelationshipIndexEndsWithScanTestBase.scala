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

abstract class RelationshipIndexEndsWithScanTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should be case sensitive for ENDS WITH with directed index scan") {
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
      .relationshipIndexOperator("(x)-[r:R(text ENDS WITH 'se')]->(y)", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = List.fill(sizeHint / 2)("case")
    runtimeResult should beColumns("text").withRows(singleColumn(expected))
  }

  test("should be case sensitive for ENDS WITH with undirected index scan") {
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
      .relationshipIndexOperator("(x)-[r:R(text ENDS WITH 'se')]-(y)", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = List.fill(sizeHint)("case")
    runtimeResult should beColumns("text").withRows(singleColumn(expected))
  }

  test("should handle null input with direction") {
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
        "(x)-[r:R(text ENDS WITH ???)]->(y)",
        paramExpr = Some(nullLiteral),
        indexType = IndexType.TEXT
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("text").withNoRows()
  }

  test("should handle null input with no direction") {
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
        "(x)-[r:R(text ENDS WITH ???)]-(y)",
        paramExpr = Some(nullLiteral),
        indexType = IndexType.TEXT
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("text").withNoRows()
  }

  test("directed scan should handle non-text input") {
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
      .projection("x.text AS text")
      .relationshipIndexOperator("(x)-[r:R(text ENDS WITH 1337)]->(y)", indexType = IndexType.TEXT)
      .build()

    execute(logicalQuery, runtime) should beColumns("text").withNoRows()
  }

  test("undirected scan should handle non-text input") {
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
      .projection("x.text AS text")
      .relationshipIndexOperator("(x)-[r:R(text ENDS WITH 1337)]-(y)", indexType = IndexType.TEXT)
      .build()

    execute(logicalQuery, runtime) should beColumns("text").withNoRows()
  }

  // We have no index that supports this query at the moment
  ignore("directed scan should cache properties") {
    val rels = given {
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
      .relationshipIndexOperator("(x)-[r:R(text ENDS WITH '1')]->(y)", _ => GetValue, indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels.zipWithIndex.collect { case (r, i) if i.toString.endsWith("1") => Array[Object](r, i.toString) }
    runtimeResult should beColumns("r", "text").withRows(expected)
  }

  // We have no index that supports this query at the moment
  ignore("undirected scan should cache properties") {
    val rels = given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("text", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "text")
      .projection("cacheR[r.text] AS text")
      .relationshipIndexOperator("(x)-[r:R(text ENDS WITH '1')]-(y)", _ => GetValue, indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels.zipWithIndex.flatMap {
      case (r, i) if i.toString.endsWith("1") =>
        Seq(
          Array[Object](r.getStartNode, r.getEndNode, i.toString),
          Array[Object](r.getEndNode, r.getStartNode, i.toString)
        )
      case _ => Seq.empty
    }
    runtimeResult should beColumns("x", "y", "text").withRows(expected)
  }

  test("should handle directed scan on the RHS of an Apply") {
    val rels = given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("text", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(text ENDS WITH '1')]->(y)", indexType = IndexType.TEXT)
      .input(variables = Seq("i"))
      .build()

    val input = (1 to 10).map(i => Array[Any](i))
    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for {
      _ <- 1 to 10
      r <- rels.filter(_.getProperty("text").asInstanceOf[String].endsWith("1"))
    } yield Array(r.getStartNode, r, r.getEndNode)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should handle undirected scan on the RHS of an Apply") {
    val rels = given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("text", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:R(text ENDS WITH '1')]-(y)", indexType = IndexType.TEXT)
      .input(variables = Seq("i"))
      .build()

    val input = (1 to 10).map(i => Array[Any](i))
    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for {
      _ <- 1 to 10
      r <- rels.filter(_.getProperty("text").asInstanceOf[String].endsWith("1"))
    } yield Seq(Array(r.getStartNode, r, r.getEndNode), Array(r.getEndNode, r, r.getStartNode))

    runtimeResult should beColumns("x", "r", "y").withRows(expected.flatten)
  }

  test("should handle directed scan and cartesian product") {
    val rels = given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("text", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2")
      .cartesianProduct()
      .|.relationshipIndexOperator("(x2)-[r2:R(text ENDS WITH '2')]->(y2)", indexType = IndexType.TEXT)
      .relationshipIndexOperator("(x1)-[r1:R(text ENDS WITH '1')]->(y1)", indexType = IndexType.TEXT)
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- rels.filter(_.getProperty("text").asInstanceOf[String].endsWith("1"))
      r2 <- rels.filter(_.getProperty("text").asInstanceOf[String].endsWith("2"))
    } yield Array(r1, r2)

    runtimeResult should beColumns("r1", "r2").withRows(expected)
  }

  test("should handle undirected scan and cartesian product") {
    val rels = given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("text", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2")
      .cartesianProduct()
      .|.relationshipIndexOperator("(x2)-[r2:R(text ENDS WITH '2')]-(y2)", indexType = IndexType.TEXT)
      .relationshipIndexOperator("(x1)-[r1:R(text ENDS WITH '1')]-(y1)", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- rels.filter(_.getProperty("text").asInstanceOf[String].endsWith("1"))
      r2 <- rels.filter(_.getProperty("text").asInstanceOf[String].endsWith("2"))
    } yield Seq.fill(4)(Array(r1, r2))
    runtimeResult should beColumns("r1", "r2").withRows(expected.flatten)
  }

  test("aggregation and limit on top of directed scan") {
    // given
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, _) => r.setProperty("text", "value")
      }
      rels
    }
    val limit = sizeHint / 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .limit(limit)
      .relationshipIndexOperator("(x)-[r:R(text ENDS WITH 'ue')]->(y)", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRows(singleRow(limit))
  }

  test("aggregation and limit on top of undirected scan") {
    // given
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, _) => r.setProperty("text", "value")
      }
      rels
    }
    val limit = sizeHint / 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .limit(limit)
      .relationshipIndexOperator("(x)-[r:R(text ENDS WITH 'ue')]->(y)", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withRows(singleRow(limit))
  }

  test("limit and directed scan on the RHS of an apply") {
    // given
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, _) => r.setProperty("text", "value")
      }
    }
    val limit = 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("value")
      .apply()
      .|.projection("r.text AS value")
      .|.limit(limit)
      .|.relationshipIndexOperator(
        "(x)-[r:R(text ENDS WITH 'alue')]->(y)",
        argumentIds = Set("i"),
        indexType = IndexType.TEXT
      )
      .input(variables = Seq("i"))
      .build()

    val input = (1 to 100).map(i => Array[Any](i))
    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("value").withRows(rowCount(limit * 100))
  }

  test("limit and undirected scan on the RHS of an apply") {
    // given
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, _) => r.setProperty("text", "value")
      }
    }
    val limit = 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("value")
      .apply()
      .|.projection("r.text AS value")
      .|.limit(limit)
      .|.relationshipIndexOperator(
        "(x)-[r:R(text ENDS WITH 'alue')]-(y)",
        argumentIds = Set("i"),
        indexType = IndexType.TEXT
      )
      .input(variables = Seq("i"))
      .build()

    val input = (1 to 100).map(i => Array[Any](i))
    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("value").withRows(rowCount(limit * 100))
  }

  test("limit on top of apply with directed scan on the RHS of an apply") {
    // given
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, _) => r.setProperty("text", "value")
      }
    }
    val limit = 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("value")
      .limit(limit)
      .apply()
      .|.projection("r.text AS value")
      .|.relationshipIndexOperator(
        "(x)-[r:R(text ENDS WITH 'alue')]->(y)",
        argumentIds = Set("i"),
        indexType = IndexType.TEXT
      )
      .input(variables = Seq("i"))
      .build()

    val input = (1 to 100).map(i => Array[Any](i))
    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("value").withRows(rowCount(limit))
  }

  test("limit on top of apply with undirected scan on the RHS of an apply") {
    // given
    given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, _) => r.setProperty("text", "value")
      }
    }
    val limit = 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("value")
      .limit(limit)
      .apply()
      .|.projection("r.text AS value")
      .|.relationshipIndexOperator(
        "(x)-[r:R(text ENDS WITH 'alue')]-(y)",
        argumentIds = Set("i"),
        indexType = IndexType.TEXT
      )
      .input(variables = Seq("i"))
      .build()

    val input = (1 to 100).map(i => Array[Any](i))
    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("value").withRows(rowCount(limit))
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
      .relationshipIndexOperator("(n)-[r:R(text ENDS WITH 'alue')]-(m)", indexType = IndexType.TEXT)
      .build()

    execute(logicalQuery, runtime) should beColumns("r").withSingleRow(rel)
  }
}
