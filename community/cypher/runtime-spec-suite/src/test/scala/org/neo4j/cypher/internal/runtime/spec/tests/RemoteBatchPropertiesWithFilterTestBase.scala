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
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.extractRuntimeConstants
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.Values
import org.scalatest.prop.TableDrivenPropertyChecks.forEvery
import org.scalatest.prop.TableFor1
import org.scalatest.prop.Tables.Table

import java.time.ZoneOffset.UTC

object RemoteBatchPropertiesWithFilterTestBase

abstract class RemoteBatchPropertiesWithFilterTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) with CypherScalaCheckDrivenPropertyChecks {

  private val nodePredicates: TableFor1[String] = Table(
    "predicate",
    "cache[x.prop1] < 10",
    "cache[x.prop1] <= 10",
    "cache[x.prop1] <> cache[x.prop2]",
    "cache[x.prop1] = $intParam",
    "cache[x.prop1] = $stringParam",
    "cache[x.prop1] = 'prop'",
    "cache[x.prop1] = 10",
    "cache[x.prop1] = cache[x.prop2]",
    "cache[x.prop1] =~ 'prop.*'",
    "cache[x.prop1] > 10",
    "cache[x.prop1] >= 10",
    "cache[x.prop1] CONTAINS 'prop'",
    "cache[x.prop1] ENDS WITH 'prop'",
    "cache[x.prop1] in $listParam",
    "cache[x.prop1] in [1,2,3]",
    "cache[x.prop1] IS :: INTEGER NOT NULL",
//    "cache[x.prop1] IS :: INTEGER",
    "cache[x.prop1] IS NOT NULL",
    "cache[x.prop1] STARTS WITH 'prop'",
    "cache[x.prop2] IS NULL",
    "labels(x)[0] = 'A'",
    "point.withinBBox(x.prop1, point({ x:1, y:1 }), point({ x:4, y:4 }))",
    "x.prop1 < 10",
    "x.prop1 <= 10",
    "x.prop1 <> x.prop2",
    "x.prop1 = $intParam",
    "x.prop1 = $stringParam",
    "x.prop1 = 'prop'",
    "x.prop1 = 10",
    "x.prop1 = x.prop2",
    "x.prop1 =~ 'prop.*'",
    "x.prop1 > 10",
    "x.prop1 >= 10",
    "x.prop1 CONTAINS 'prop'",
    "x.prop1 ENDS WITH 'prop'",
    "x.prop1 in $listParam",
    "x.prop1 in [1,2,3]",
    "x.prop1 IS :: INTEGER NOT NULL",
//    "x.prop1 IS :: INTEGER",
    "x.prop1 IS NOT NULL",
    "x.prop1 STARTS WITH 'prop'",
    "x.prop2 IS NULL",
    "x:A AND NOT x:B",
    "x:A AND x:B",
    "x:A OR size(labels(x)) > 1",
    "x:A OR x.prop > 20",
    "x:A OR x:B",
    "x:A",
    "x:A|B"
  )

  private val relationshipPredicates = Table(
    "predicates",
    "cacheR[x.prop1] < 10",
    "cacheR[x.prop1] <= 10",
    "cacheR[x.prop1] = $intParam",
    "cacheR[x.prop1] = $stringParam",
    "cacheR[x.prop1] = 'prop'",
    "cacheR[x.prop1] = 10",
    "cacheR[x.prop1] =~ 'prop.*'",
    "cacheR[x.prop1] > 10",
    "cacheR[x.prop1] >= 10",
    "cacheR[x.prop1] CONTAINS 'prop'",
    "cacheR[x.prop1] ENDS WITH 'prop'",
    "cacheR[x.prop1] IS NOT NULL",
    "cacheR[x.prop1] STARTS WITH 'prop'",
    "cacheR[x.prop1] in $listParam",
    "cacheR[x.prop1] in [1,2,3]",
    "point.withinBBox(x.prop1, point({ x:1, y:1 }), point({ x:4, y:4 }))",
    "type(x) = 'A'",
    "type(x) IN ['A', 'B']",
    "x.prop1 < 10",
    "x.prop1 <= 10",
    "x.prop1 = $intParam",
    "x.prop1 = $stringParam",
    "x.prop1 = 'prop'",
    "x.prop1 = 10",
    "x.prop1 =~ 'prop.*'",
    "x.prop1 > 10",
    "x.prop1 >= 10",
    "x.prop1 CONTAINS 'prop'",
    "x.prop1 ENDS WITH 'prop'",
    "x.prop1 IS NOT NULL",
    "x.prop1 STARTS WITH 'prop'",
    "x.prop1 in $listParam",
    "x.prop1 in [1,2,3]",
    "x:A AND x.prop > 10",
    "x:A AND x:B",
    "x:A OR x:B",
    "x:A",
    "x:A|B"
  )

  test("should return nothing - on empty graph - for all predicates") {

    forEvery(nodePredicates) { (predicate: String) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop1")
          .projection("cache[x.prop1] as prop1")
          .remoteBatchPropertiesWithFilter("cache[x.prop1]")(predicate)
          .allNodeScan("x")
          .build()

        val result =
          execute(
            query,
            runtime,
            Map[String, Any]("stringParam" -> "a string", "intParam" -> 10, "listParam" -> Array(1, 2, 3))
          )
        result should beColumns("prop1").withNoRows()
      }
    }
  }

  test("should return nothing on empty node") {
    givenGraph {
      tx.createNode()
    }

    forEvery(nodePredicates) { (predicate: String) =>
      val expected: List[Array[AnyRef]] =
        if (predicate.contains("IS NULL")) List(Array(Values.NO_VALUE)) else List.empty[Array[AnyRef]]

      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop1")
          .projection("cache[x.prop1] as prop1")
          .remoteBatchPropertiesWithFilter("cache[x.prop1]")(predicate)
          .allNodeScan("x")
          .build()

        val result =
          execute(
            query,
            runtime,
            Map[String, Any]("stringParam" -> "a string", "intParam" -> 10, "listParam" -> Array(1, 2, 3))
          )
        result should beColumns("prop1").withRows(expected)
      }
    }
  }

  test("should return nothing on empty relationship") {
    givenGraph {
      lineGraph(2, "R")
    }

    forEvery(relationshipPredicates) { (predicate: String) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop1")
          .projection("cacheR[x.prop1] as prop1")
          .remoteBatchPropertiesWithFilter("cacheR[x.prop1]")(
            predicate
          )
          .allRelationshipsScan("()-[x]->()")
          .build()

        val result =
          execute(
            query,
            runtime,
            Map[String, Any]("stringParam" -> "a string", "intParam" -> 10, "listParam" -> Array(1, 2, 3))
          )
        result should beColumns("prop1").withNoRows()
      }
    }
  }

  // ------------------------------------------------
  // return nothing on non-existing property - on tiny graph
  // ------------------------------------------------

  test("should return nothing on non-existing node property - on tiny graph - for all predicates") {

    givenGraph {
      tx.createNode().setProperty("prop2", 10)
      tx.createNode().setProperty("prop2", 20)
      tx.createNode().setProperty("prop2", 30)
    }

    forEvery(nodePredicates) { (predicate: String) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop1", "prop2")
          .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
          .remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]")(
            predicate
          )
          .allNodeScan("x")
          .build()

        val result =
          execute(
            query,
            runtime,
            Map[String, Any]("stringParam" -> "a string", "intParam" -> 10, "listParam" -> Array(1, 2, 3))
          )
        result should beColumns("prop1", "prop2").withNoRows()
      }
    }
  }

  test("should return nothing on non-existing relationship property - on tiny graph - for all predicates") {

    givenGraph {
      val (_, rels) = lineGraph(4, "R")
      rels(0).setProperty("prop2", 10)
      rels(1).setProperty("prop2", 20)
      rels(2).setProperty("prop2", 30)
    }

    forEvery(relationshipPredicates) { (predicate: String) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop1", "prop2")
          .projection("cacheR[x.prop1] as prop1", "cacheR[x.prop2] as prop2")
          .remoteBatchPropertiesWithFilter("cacheR[x.prop1]", "cacheR[x.prop2]")(
            predicate
          )
          .expandAll("(n)-[x:R]->()")
          .allNodeScan("n")
          .build()

        val result =
          execute(
            query,
            runtime,
            Map[String, Any]("stringParam" -> "a string", "intParam" -> 10, "listParam" -> Array(1, 2, 3))
          )
        result should beColumns("prop1", "prop2").withNoRows()
      }
    }
  }

  // -------------------------------------------

  test(
    "should return nothing on mix of existing and non-existing properties- prop1 IS NOT NULL and prop2 Equals - on tiny graph"
  ) {

    givenGraph {
      tx.createNode().setProperty("prop2", 10)
      tx.createNode().setProperty("prop2", 20)
      tx.createNode().setProperty("prop2", 30)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]")(
        "cache[x.prop1] IS NOT NULL",
        "cache[x.prop2] = 10"
      )
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    result should beColumns("prop1", "prop2").withNoRows()
  }

  // ------------------------------------------------
  // one property column - on tiny graph
  // ------------------------------------------------

  private val propertyPredicates = Table(
    ("predicate", "expected"),
    ("cache[x.prop] < 20", Seq(10)),
    ("cache[x.prop] <= 20", Seq(10, 20)),
    ("cache[x.prop] <> 20", Seq(10, 30)),
    ("cache[x.prop] <> cache[x.prop]", Seq.empty),
    ("cache[x.prop] = $param", Seq(20)),
    ("cache[x.prop] = 20", Seq(20)),
    ("cache[x.prop] = cache[x.prop]", Seq(10, 20, 30)),
    ("cache[x.prop] > 20", Seq(30)),
    ("cache[x.prop] >= 20", Seq(20, 30)),
    ("cache[x.prop] IN $listParam", Seq(10, 30)),
    ("cache[x.prop] IN [10, 30]", Seq(10, 30)),
    ("cache[x.prop] IS NOT NULL", Seq(10, 20, 30)),
    ("cache[x.prop] IS NULL", Seq.empty),
    ("NOT cache[x.prop] = 20", Seq(10, 30)),
    ("NOT cache[x.prop] IS NOT NULL", Seq.empty),
    ("NOT cache[x.prop] IS NULL", Seq(10, 20, 30)),
    ("NOT x.prop = 20", Seq(10, 30)),
    ("NOT x.prop IS NOT NULL", Seq.empty),
    ("NOT x.prop IS NULL", Seq(10, 20, 30)),
    ("x <> x", Seq.empty),
    ("x = x", Seq(10, 20, 30)),
    ("NOT x = x", Seq.empty),
    ("x.prop < $param", Seq(10)),
    ("x.prop < 20", Seq(10)),
    ("x.prop <= $param", Seq(10, 20)),
    ("x.prop <= 20", Seq(10, 20)),
    ("x.prop <> 20", Seq(10, 30)),
    ("x.prop <> x.prop", Seq.empty),
    ("x.prop = $param + 10", Seq(30)),
    ("x.prop = $param", Seq(20)),
    ("x.prop = 20", Seq(20)),
    ("x.prop = x.prop", Seq(10, 20, 30)),
    ("x.prop > $param", Seq(30)),
    ("x.prop > 20", Seq(30)),
    ("x.prop >= $param", Seq(20, 30)),
    ("x.prop >= 10 + $param", Seq(30)),
    ("x.prop >= 20", Seq(20, 30)),
    ("x.prop IN $listParam", Seq(10, 30)),
    ("x.prop IN [10, 30]", Seq(10, 30)),
    ("x.prop IS NOT NULL", Seq(10, 20, 30)),
    ("x.prop IS NULL", Seq.empty),
    ("TRUE", Seq(10, 20, 30)),
    ("FALSE", Seq.empty)
  )

  test("should return one node property column - on tiny graph - numeric values") {
    givenGraph {
      tx.createNode().setProperty("prop", 10)
      tx.createNode().setProperty("prop", 20)
      tx.createNode().setProperty("prop", 30)

    }

    forEvery(propertyPredicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cache[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cache[x.prop]")(predicate)
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> 20, "listParam" -> Array(10, 30)))
        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should return two node property columns when under Apply with constant predicate") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i, "prop2" -> i * 2) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .|.remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]")("TRUE")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).map(i => Array(i, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should work with projections and multiple predicates") {
    givenGraph {
      tx.createNode().setProperty("prop", 10)
      tx.createNode().setProperty("prop", 20)
      tx.createNode().setProperty("prop", 30)

    }

    forEvery(propertyPredicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cache[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cache[x.prop]")("x IS NOT NULL", predicate)
          .projection("y as x")
          .allNodeScan("y")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> 20, "listParam" -> Array(10, 30)))
        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should work with expand and projection") {
    givenGraph {
      val startNode = tx.createNode(Label.label("START"))
      val n1 = tx.createNode()
      n1.setProperty("prop", 10)
      val n2 = tx.createNode()
      n2.setProperty("prop", 20)
      val n3 = tx.createNode()
      n3.setProperty("prop", 30)

      startNode.createRelationshipTo(n1, RelationshipType.withName("R"))
      startNode.createRelationshipTo(n2, RelationshipType.withName("R"))
      startNode.createRelationshipTo(n3, RelationshipType.withName("R"))

    }

    forEvery(propertyPredicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cache[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cache[x.prop]")(predicate)
          .projection("y as x")
          .expandAll("(z)-[]->(y)")
          .nodeByLabelScan("z", "START")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> 20, "listParam" -> Array(10, 30)))

        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should return one relationship property column - on tiny graph") {

    givenGraph {
      val (_, rels) = lineGraph(4, "R")
      rels(0).setProperty("prop", 10)
      rels(1).setProperty("prop", 20)
      rels(2).setProperty("prop", 30)
    }

    val relationshipPredicates = propertyPredicates.map(p => (p._1.replace("cache", "cacheR"), p._2))
    val predicates = Table(("predicate", "expected"), relationshipPredicates: _*)

    forEvery(predicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cacheR[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cacheR[x.prop]")(predicate)
          .expandAll("(n)-[x:R]->()")
          .allNodeScan("n")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> 20, "listParam" -> Array(10, 30)))
        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should work with expand and relationship projection") {
    givenGraph {
      val (_, rels) = lineGraph(4, "R")
      rels(0).setProperty("prop", 10)
      rels(1).setProperty("prop", 20)
      rels(2).setProperty("prop", 30)
    }

    val relationshipPredicates = propertyPredicates.map(p => (p._1.replace("cache", "cacheR"), p._2))
    val predicates = Table(("predicate", "expected"), relationshipPredicates: _*)

    forEvery(predicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cacheR[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cacheR[x.prop]")(predicate)
          .projection("r as x")
          .expandAll("(z)-[r]->(y)")
          .allNodeScan("z")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> 20, "listParam" -> Array(10, 30)))

        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should return one node property column - on tiny graph - string values") {
    givenGraph {
      tx.createNode().setProperty("prop", "prop1")
      tx.createNode().setProperty("prop", "prop2")
      tx.createNode().setProperty("prop", "prop3")

      tx.createNode().setProperty("prop", "1prop")
      tx.createNode().setProperty("prop", "2prop")
      tx.createNode().setProperty("prop", "3prop")

      tx.createNode().setProperty("prop", "1prp")
      tx.createNode().setProperty("prop", "prp1")
      tx.createNode().setProperty("prop", "1prp1")

    }

    val predicates = Table(
      ("predicate", "expected"),
      ("x.prop STARTS WITH 'prop'", Seq("prop1", "prop2", "prop3")),
      ("x.prop STARTS WITH $param", Seq("prop1", "prop2", "prop3")),
      ("x.prop ENDS WITH 'prop'", Seq("1prop", "2prop", "3prop")),
      ("x.prop ENDS WITH $param", Seq("1prop", "2prop", "3prop")),
      ("x.prop CONTAINS 'prp'", Seq("1prp", "prp1", "1prp1")),
      ("x.prop CONTAINS $contains", Seq("1prp", "prp1", "1prp1")),
      ("x.prop =~ '1pr.*'", Seq("1prop", "1prp", "1prp1")),
      ("cache[x.prop] STARTS WITH 'prop'", Seq("prop1", "prop2", "prop3")),
      ("cache[x.prop] STARTS WITH $param", Seq("prop1", "prop2", "prop3")),
      ("cache[x.prop] ENDS WITH 'prop'", Seq("1prop", "2prop", "3prop")),
      ("cache[x.prop] ENDS WITH $param", Seq("1prop", "2prop", "3prop")),
      ("cache[x.prop] CONTAINS 'prp'", Seq("1prp", "prp1", "1prp1")),
      ("cache[x.prop] CONTAINS $contains", Seq("1prp", "prp1", "1prp1")),
      ("cache[x.prop] =~ '1pr.*'", Seq("1prop", "1prp", "1prp1"))
    )

    forEvery(predicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cache[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cache[x.prop]")(predicate)
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> "prop", "contains" -> "prp"))
        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should return one relationship property column - on tiny graph - string values") {
    givenGraph {
      val (_, rels) = lineGraph(10, "R")
      rels(0).setProperty("prop", "prop1")
      rels(1).setProperty("prop", "prop2")
      rels(2).setProperty("prop", "prop3")

      rels(3).setProperty("prop", "1prop")
      rels(4).setProperty("prop", "2prop")
      rels(5).setProperty("prop", "3prop")

      rels(6).setProperty("prop", "1prp")
      rels(7).setProperty("prop", "prp1")
      rels(8).setProperty("prop", "1prp1")

    }

    val predicates = Table(
      ("predicate", "expected"),
      ("x.prop STARTS WITH 'prop'", Seq("prop1", "prop2", "prop3")),
      ("x.prop STARTS WITH $param", Seq("prop1", "prop2", "prop3")),
      ("x.prop ENDS WITH 'prop'", Seq("1prop", "2prop", "3prop")),
      ("x.prop ENDS WITH $param", Seq("1prop", "2prop", "3prop")),
      ("x.prop CONTAINS 'prp'", Seq("1prp", "prp1", "1prp1")),
      ("x.prop CONTAINS $contains", Seq("1prp", "prp1", "1prp1")),
      ("x.prop =~ '1pr.*'", Seq("1prop", "1prp", "1prp1")),
      ("cacheR[x.prop] STARTS WITH 'prop'", Seq("prop1", "prop2", "prop3")),
      ("cacheR[x.prop] STARTS WITH $param", Seq("prop1", "prop2", "prop3")),
      ("cacheR[x.prop] ENDS WITH 'prop'", Seq("1prop", "2prop", "3prop")),
      ("cacheR[x.prop] ENDS WITH $param", Seq("1prop", "2prop", "3prop")),
      ("cacheR[x.prop] CONTAINS 'prp'", Seq("1prp", "prp1", "1prp1")),
      ("cacheR[x.prop] CONTAINS $contains", Seq("1prp", "prp1", "1prp1")),
      ("cacheR[x.prop] =~ '1pr.*'", Seq("1prop", "1prp", "1prp1"))
    )

    forEvery(predicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cacheR[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cacheR[x.prop]")(predicate)
          .expandAll("(n)-[x:R]->()")
          .allNodeScan("n")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> "prop", "contains" -> "prp"))
        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should return one node property column - on tiny graph - STARTS WITH AND ENDS WITH") {
    givenGraph {
      tx.createNode().setProperty("prop", "112233")
      tx.createNode().setProperty("prop", "223344")
      tx.createNode().setProperty("prop", "11")
      tx.createNode().setProperty("prop", "33")
      tx.createNode().setProperty("prop", "1133")
      tx.createNode().setProperty("prop", "1122")
      tx.createNode().setProperty("prop", "2233")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[x.prop] as prop")
      .remoteBatchPropertiesWithFilter("cache[x.prop]")(
        "x.prop STARTS WITH $param1",
        "x.prop ENDS WITH $param2"
      )
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime, Map("param1" -> "11", "param2" -> "33"))
    result should beColumns("prop").withRows(singleColumn(Seq("112233", "1133")))
  }

  test("should return one relationship property column - on tiny graph - STARTS WITH AND ENDS WITH") {
    givenGraph {
      val (_, rels) = lineGraph(8, "R")
      rels(0).setProperty("prop", "112233")
      rels(1).setProperty("prop", "223344")
      rels(2).setProperty("prop", "11")
      rels(3).setProperty("prop", "33")
      rels(4).setProperty("prop", "1133")
      rels(5).setProperty("prop", "1122")
      rels(6).setProperty("prop", "2233")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cacheR[x.prop] as prop")
      .remoteBatchPropertiesWithFilter("cacheR[x.prop]")("x.prop STARTS WITH $param1", "x.prop ENDS WITH $param2")
      .expandAll("(n)-[x:R]->()")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime, Map("param1" -> "11", "param2" -> "33"))
    result should beColumns("prop").withRows(singleColumn(Seq("112233", "1133")))
  }

  test("should return one node property column - on tiny graph - point values") {
    givenGraph {
      tx.createNode().setProperty("prop", PointValue.parse("{x:2, y:0}"))
      tx.createNode().setProperty("prop", PointValue.parse("{x:2, y:2}"))
      tx.createNode().setProperty("prop", PointValue.parse("{x:3, y:3}"))
      tx.createNode().setProperty("prop", PointValue.parse("{x:5, y:5}"))
    }

    val predicates = Table(
      "predicate",
      "point.withinBBox(x.prop, point({ x:1, y:1 }), point({ x:4, y:4 }))",
      "point.withinBBox(x.prop, point({x: $x1, y: $y1}), point({x: $x2, y: $y2}))"
    )
    forEvery(predicates) { (predicate: String) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cache[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cache[x.prop]")(
            predicate
          )
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime, Map("x1" -> 1, "y1" -> 1, "x2" -> 4, "y2" -> 4))
        result should beColumns("prop").withRows(singleColumn(Seq(
          PointValue.parse("{x: 2.0, y: 2.0}"),
          PointValue.parse("{x: 3.0, y: 3.0}")
        )))
      }
    }
  }

  test("should return one relationship property column - on tiny graph - point values") {
    givenGraph {
      val (_, rels) = lineGraph(5, "R")
      rels(0).setProperty("prop", PointValue.parse("{x:2, y:0}"))
      rels(1).setProperty("prop", PointValue.parse("{x:2, y:2}"))
      rels(2).setProperty("prop", PointValue.parse("{x:3, y:3}"))
      rels(3).setProperty("prop", PointValue.parse("{x:5, y:5}"))
    }

    val predicates = Table(
      "predicate",
      "point.withinBBox(x.prop, point({ x:1, y:1 }), point({ x:4, y:4 }))",
      "point.withinBBox(x.prop, point({x: $x1, y: $y1}), point({x: $x2, y: $y2}))"
    )
    forEvery(predicates) { (predicate: String) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cacheR[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cacheR[x.prop]")(predicate)
          .expandAll("(n)-[x:R]->()")
          .allNodeScan("n")
          .build()

        val result = execute(query, runtime, Map("x1" -> 1, "y1" -> 1, "x2" -> 4, "y2" -> 4))
        result should beColumns("prop").withRows(singleColumn(Seq(
          PointValue.parse("{x: 2.0, y: 2.0}"),
          PointValue.parse("{x: 3.0, y: 3.0}")
        )))
      }
    }
  }

  test("should work with projected point values") {

    givenGraph {
      nodePropertyGraph(
        10,
        { case i => Map("prop" -> PointValue.parse(s"{x:$i, y:${i + 1}}")) },
        "Foo"
      )
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("count")
      .aggregation(Seq(), Seq("count(cache[n.prop]) AS count"))
      .remoteBatchPropertiesWithFilter("cache[n.prop]")("n.prop = value")
      .apply()
      .|.nodeByLabelScan("n", "Foo", IndexOrderNone, "value")
      .unwind("[point({x: $x, y: $y })] AS value")
      .argument()
      .build()

    val result = execute(query, runtime, Map("x" -> 4, "y" -> 5))

    result should beColumns("count").withSingleRow(1)
  }

  test("should work with projected list of point values") {

    givenGraph {
      nodePropertyGraph(
        10,
        { case i => Map("prop" -> PointValue.parse(s"{x:$i, y:${i + 1}}")) },
        "Foo"
      )
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("count")
      .aggregation(Seq(), Seq("count(prop) AS count"))
      .distinct("cache[n.prop] as prop")
      .remoteBatchPropertiesWithFilter("cache[n.prop]")("n.prop IN nestedPointArray")
      .apply()
      .|.nodeByLabelScan("n", "Foo", IndexOrderNone, "nestedPointArray")
      .unwind(
        "[[point({x: 4, y: 5 }), point({x: 5, y: 6})], " +
          "[point({x: 5, y: 6}), point({x: 1, y: 2})], " +
          "[point({x: 5, y: 6}), point({x: 3, y: 2})]] AS nestedPointArray"
      )
      .argument()
      .build()

    val result = execute(query, runtime)

    result should beColumns("count").withSingleRow(3)
  }

  // -------------------------------------------------------

  test("should return one node property column - on tiny graph - range text value") {
    givenGraph {
      tx.createNode().setProperty("prop", "a")
      tx.createNode().setProperty("prop", "b")
      tx.createNode().setProperty("prop", "c")
      tx.createNode().setProperty("prop", "d")
    }

    val predicates = Table(
      ("predicate", "expected"),
      ("x.prop > $param", Seq("c", "d")),
      ("x.prop >= $param", Seq("b", "c", "d")),
      ("x.prop < $param", Seq("a")),
      ("x.prop <= $param", Seq("a", "b")),
      ("cache[x.prop] > $param", Seq("c", "d")),
      ("cache[x.prop] >= $param", Seq("b", "c", "d")),
      ("cache[x.prop] < $param", Seq("a")),
      ("cache[x.prop] <= $param", Seq("a", "b"))
    )

    forEvery(predicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cache[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cache[x.prop]")(predicate)
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> "b"))
        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should return one node property column - on tiny graph - range less than AND greater than") {
    givenGraph {
      tx.createNode().setProperty("prop", 10)
      tx.createNode().setProperty("prop", 15)
      tx.createNode().setProperty("prop", 20)
      tx.createNode().setProperty("prop", 25)

    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[x.prop] as prop")
      .remoteBatchPropertiesWithFilter("cache[x.prop]")("cache[x.prop] > 10", "cache[x.prop] < 20")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    result should beColumns("prop").withRows(singleColumn(Seq(15)))
  }

  test("should return one relationship property column - on tiny graph - range less than AND greater than") {
    givenGraph {
      val (_, rels) = lineGraph(5, "R")
      rels(0).setProperty("prop", 10)
      rels(1).setProperty("prop", 15)
      rels(2).setProperty("prop", 20)
      rels(3).setProperty("prop", 25)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cacheR[x.prop] as prop")
      .remoteBatchPropertiesWithFilter("cacheR[x.prop]")("cacheR[x.prop] > 10", "cacheR[x.prop] < 20")
      .expandAll("(n)-[x:R]->()")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime)
    result should beColumns("prop").withRows(singleColumn(Seq(15)))
  }

  test("should handle datetime() function as RuntimeConstant") {
    val expectedDate = DateTimeValue.parse("2024-01-01T00:00", () => UTC)
    givenGraph {
      tx.createNode().setProperty("created", expectedDate)
      tx.createNode().setProperty("created", DateTimeValue.parse("2025-01-01T00:00", () => UTC))
      tx.createNode().setProperty("created", DateTimeValue.parse("2026-01-01T00:00", () => UTC))
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("created")
      .projection("cache[x.created] as created")
      .remoteBatchPropertiesWithFilter("cache[x.created]")("cache[x.created] < datetime({date: $param})")
      .allNodeScan("x")
      .build()

    val rewrittenQuery = extractRuntimeConstants(new AnonymousVariableNameGenerator)(query).asInstanceOf[LogicalQuery]

    val result = execute(rewrittenQuery, runtime, Map[String, Any]("param" -> DateValue.date(2025, 1, 1)))
    result should beColumns("created").withRows(singleColumn(Seq(expectedDate)))
  }

  private val labelPredicates =
    Table(
      ("predicate", "expected"),
      ("x:A", Seq(10, 40)),
      ("x:A AND NOT x:B", Seq(10)),
      ("x:A OR x:C", Seq(10, 30, 40)),
      ("x:A AND x:B", Seq(40)),
      ("x:A OR (x:B AND x.prop > 30)", Seq(10, 40)),
      ("x:A AND x.prop > 10", Seq(40)),
      ("x:C AND x.prop > 30", Seq.empty),
      ("x:A OR x.prop > 20", Seq(10, 30, 40)),
      ("x:C OR size(labels(x)) > 1", Seq(30, 40)),
      ("labels(x)[0] = 'C'", Seq(30)),
      ("labels(x)[1] = 'F'", Seq.empty),
      ("x:A&B", Seq(40)),
      ("x:A|B", Seq(10, 20, 40)),
      ("x:%", Seq(10, 20, 30, 40)),
      ("x:!A", Seq(20, 30)),
      ("x:!%", Seq.empty),
      ("x:A&%", Seq(10, 40))
    )

  test("should match node labels") {
    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("prop", 10)
      val n2 = tx.createNode(Label.label("B"))
      n2.setProperty("prop", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("prop", 30)
      val n4 = tx.createNode(Label.label("A"), Label.label("B"))
      n4.setProperty("prop", 40)
    }

    forEvery(labelPredicates) { (predicate, expected) =>
      val builder = new LogicalQueryBuilder(this)
      val query = builder
        .produceResults("prop")
        .projection("cache[x.prop] as prop")
        .remoteBatchPropertiesWithFilter("cache[x.prop]")(predicate)
        .allNodeScan("x")
        .build()

      val result = execute(query, runtime)

      result should beColumns("prop").withRows(singleColumn(expected))
    }
  }

  test("should match node labels - with extra slots") {
    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("prop", 10)
      val n2 = tx.createNode(Label.label("B"))
      n2.setProperty("prop", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("prop", 30)
      val n4 = tx.createNode(Label.label("A"), Label.label("B"))
      n4.setProperty("prop", 40)
    }

    forEvery(labelPredicates) { (predicate, expected) =>
      val builder = new LogicalQueryBuilder(this)
      val query = builder
        .produceResults("prop")
        .apply()
        .|.projection("cache[x.prop] as prop")
        .|.remoteBatchPropertiesWithFilter("cache[x.prop]")(predicate)
        .|.argument("x")
        .distinct("x as x")
        .apply()
        .|.allNodeScan("x")
        .unwind("[1] as a")
        .allNodeScan("y")
        .build()

      val result = execute(query, runtime)

      result should beColumns("prop").withRows(singleColumn(expected))
    }
  }

  private val relationshipTypePredicates = Table(
    ("predicate", "expected"),
    ("x:A", Seq(10, 40)),
    ("x:A OR x:B", Seq(10, 20, 40)),
    ("x:A AND x:B", Seq.empty),
    ("x:A AND x.prop > 10", Seq(40)),
    ("x:C AND x.prop > 30", Seq.empty),
    ("x:A OR x.prop > 20", Seq(10, 30, 40)),
    ("type(x) = 'A' OR x.prop = 30", Seq(10, 30, 40)),
    ("type(x) IN ['B', 'C']", Seq(20, 30)),
    ("x:A&B", Seq.empty),
    ("x:A|C", Seq(10, 30, 40)),
    ("x:A|%", Seq(10, 20, 30, 40)),
    ("x:A|!%", Seq(10, 40))
  )

  test("should match relationship types") {
    givenGraph {
      val nodes = nodeGraph(5)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("A")).setProperty("prop", 10)
      nodes(1).createRelationshipTo(nodes(2), RelationshipType.withName("B")).setProperty("prop", 20)
      nodes(2).createRelationshipTo(nodes(3), RelationshipType.withName("C")).setProperty("prop", 30)
      nodes(3).createRelationshipTo(nodes(4), RelationshipType.withName("A")).setProperty("prop", 40)
    }

    forEvery(relationshipTypePredicates) { (predicate, expected) =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("cacheR[x.prop] as prop")
        .remoteBatchPropertiesWithFilter("cacheR[x.prop]")(predicate)
        .allRelationshipsScan("()-[x]->()")
        .build()

      val result = execute(query, runtime)
      result should beColumns("prop").withRows(singleColumn(expected))
    }
  }

  test("should match relationship types - with extra slots") {
    givenGraph {
      val nodes = nodeGraph(5)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("A")).setProperty("prop", 10)
      nodes(1).createRelationshipTo(nodes(2), RelationshipType.withName("B")).setProperty("prop", 20)
      nodes(2).createRelationshipTo(nodes(3), RelationshipType.withName("C")).setProperty("prop", 30)
      nodes(3).createRelationshipTo(nodes(4), RelationshipType.withName("A")).setProperty("prop", 40)
    }

    forEvery(relationshipTypePredicates) { (predicate, expected) =>
      val query = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("cacheR[x.prop] as prop")
        .remoteBatchPropertiesWithFilter("cacheR[x.prop]")(predicate)
        .distinct("x as x")
        .apply()
        .|.allRelationshipsScan("(start)-[x]->(end)")
        .allNodeScan("y")
        .build()

      val result = execute(query, runtime)
      result should beColumns("prop").withRows(singleColumn(expected))
    }
  }

  // ------------------------------------
  // two node property columns
  // ------------------------------------

  test("should return two node property columns - on tiny graph") {
    givenGraph {
      val n1 = tx.createNode()
      n1.setProperty("prop1", 10)
      n1.setProperty("prop2", 11)
      val n2 = tx.createNode()
      n2.setProperty("prop1", 20)
      // n2.prop2 is null
      val n3 = tx.createNode()
      n3.setProperty("prop1", 20)
      n3.setProperty("prop2", 20)
      val n4 = tx.createNode()
      n4.setProperty("prop1", "one")
      n4.setProperty("prop2", "two")
    }

    val rowN1 = Array(10, 11)
    val rowN2 = Array[Any](20, null)
    val rowN3 = Array(20, 20)
    val rowN4 = Array("one", "two")

    val predicates = Table(
      ("predicate(s)", "expected"),
      (Seq("x.prop1 = 20"), Seq(rowN2, rowN3)),
      (Seq("x.prop1 = 20", "x.prop2 = 20"), Seq(rowN3)),
      (Seq("x.prop2 = 20"), Seq(rowN3)),
      (Seq("x.prop1 IS NOT NULL"), Seq(rowN1, rowN2, rowN3, rowN4)),
      (Seq("x.prop2 IS NOT NULL"), Seq(rowN1, rowN3, rowN4)),
      (Seq("x.prop1 IS NULL"), Seq()),
      (Seq("x.prop2 IS NULL"), Seq(rowN2)),
      (Seq("x.prop1 <> x.prop2"), Seq(rowN1, rowN4)),
      (Seq("x.prop1 = x.prop2"), Seq(rowN3)),
      (Seq("x.prop1 IS :: INTEGER"), Seq(rowN1, rowN2, rowN3)),
      (Seq("x.prop2 IS :: INTEGER"), Seq(rowN1, rowN2, rowN3)),
      (Seq("x.prop2 IS :: INTEGER NOT NULL"), Seq(rowN1, rowN3)),
      (Seq("cache[x.prop1] = 20"), Seq(rowN2, rowN3)),
      (Seq("cache[x.prop1] = 20", "cache[x.prop2] = 20"), Seq(rowN3)),
      (Seq("cache[x.prop2] = 20"), Seq(rowN3)),
      (Seq("cache[x.prop1] IS NOT NULL"), Seq(rowN1, rowN2, rowN3, rowN4)),
      (Seq("cache[x.prop2] IS NOT NULL"), Seq(rowN1, rowN3, rowN4)),
      (Seq("cache[x.prop1] IS NULL"), Seq()),
      (Seq("cache[x.prop2] IS NULL"), Seq(rowN2)),
      (Seq("cache[x.prop1] <> cache[x.prop2]"), Seq(rowN1, rowN4)),
      (Seq("cache[x.prop1] = cache[x.prop2]"), Seq(rowN3)),
      (Seq("cache[x.prop1] IS :: INTEGER"), Seq(rowN1, rowN2, rowN3)),
      (Seq("cache[x.prop2] IS :: INTEGER"), Seq(rowN1, rowN2, rowN3)),
      (Seq("cache[x.prop2] IS :: INTEGER NOT NULL"), Seq(rowN1, rowN3))
    )

    forEvery(predicates) { (predicate: Seq[String], expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop1", "prop2")
          .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
          .remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]")(predicate: _*)
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime)
        result should beColumns("prop1", "prop2").withRows(expected)
      }
    }
  }

  test("should return two node properties columns - Equals") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i % 2, "prop2" -> i * 2) })
    }

    val predicates = Table(
      "predicate",
      "x.prop1 = 0",
      "cache[x.prop1] = 0",
      "cache[x.prop1] = $param"
    )

    forEvery(predicates) { (predicate: String) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop1", "prop2")
          .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
          .remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]")(predicate)
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> 0))
        val expected = (0 until sizeHint).filter(_ % 2 == 0).map(i => Array(0, i * 2))
        result should beColumns("prop1", "prop2").withRows(expected)
      }
    }
  }

  test("should return two node properties columns - prop1 Equals AND prop2 IS NOT NULL") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 3 == 0 => Map("prop1" -> true, "prop2" -> i)
          case i if i % 3 == 1 => Map("prop1" -> false, "prop2" -> i)
          case i if i % 3 == 2 => Map("prop1" -> true)
        }
      )
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]")(
        "cache[x.prop1] = true",
        "cache[x.prop2] IS NOT NULL"
      )
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected: Seq[Array[Any]] = (0 until sizeHint).filter(_ % 3 == 0) map (i => Array(true, i))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  // ------------------------------------

  test("should return two node properties columns when split into two plans with/without filter - Equals") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i % 2, "prop2" -> i * 2) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .remoteBatchProperties("cache[x.prop2]")
      .remoteBatchPropertiesWithFilter("cache[x.prop1]")("cache[x.prop1] = 0")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).filter(_ % 2 == 0).map(i => Array(0, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return three node properties columns with nulls - IS NOT NULL") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 4 == 0 => Map("prop1" -> i, "prop2" -> i * 2, "prop3" -> i * 3)
          case i if i % 4 == 1 => Map("prop1" -> i, "prop3" -> i * 3)
          case i if i % 4 == 2 => Map("prop2" -> i * 2, "prop3" -> i * 3)
          case i if i % 4 == 3 => Map("prop1" -> i, "prop2" -> i * 2)
        }
      )
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2", "prop3")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2", "cache[x.prop3] as prop3")
      .remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]", "cache[x.prop3]")(
        "cache[x.prop3] IS NOT NULL"
      )
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected: Seq[Array[Any]] = (0 until sizeHint).flatMap {
      case i if i % 4 == 0 => Some(Array(i, i * 2, i * 3))
      case i if i % 4 == 1 => Some(Array(i, null, i * 3))
      case i if i % 4 == 2 => Some(Array(null, i * 2, i * 3))
      case i if i % 4 == 3 => None
    }
    result should beColumns("prop1", "prop2", "prop3").withRows(expected)
  }

  test("should work with deduplicated node names on union - on tiny graph") {

    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("prop", 10)
      n1.setProperty("maybeProp", true)
      val n2 = tx.createNode()
      n2.setProperty("prop", 20)
      val n3 = tx.createNode()
      n3.setProperty("prop", 30)
      n3.setProperty("maybeProp", true)
    }

    val predicates = Table(
      ("predicate", "expected"),
      ("`  x@10`.prop < 20", Seq(10, 30)),
      ("cache[`  x@10`.prop] < 20", Seq(10, 30)),
      ("`  x@10`.maybeProp IS NOT NULL", Seq(10, 30, 30)),
      ("`  x@10`.maybeProp IS NULL", Seq(20, 30)),
      ("cache[`  x@10`.maybeProp] IS NOT NULL", Seq(10, 30, 30)),
      ("cache[`  x@10`.maybeProp] IS NULL", Seq(20, 30)),
      ("`  x@10`.notProp IS NOT NULL", Seq(30)),
      ("`  x@10`.notProp IS NULL", Seq(10, 20, 30, 30)),
      ("cache[`  x@10`.notProp] IS NOT NULL", Seq(30)),
      ("cache[`  x@10`.notProp] IS NULL", Seq(10, 20, 30, 30)),
      ("`  x@10`:A", Seq(10, 30)),
      ("NOT `  x@10`:A", Seq(20, 30, 30)),
      ("`  x@10`.prop IS :: INTEGER", Seq(10, 20, 30, 30)),
      ("cache[`  x@10`.prop] IS :: INTEGER", Seq(10, 20, 30, 30))
    )
    forEvery(predicates) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .union() // no distinct on top of union, will produce duplicates from lhs and rhs
          .|.projection("cache[`  x@10`.prop] as prop")
          .|.remoteBatchPropertiesWithFilter("cache[`  x@10`.prop]")(predicate)
          .|.allNodeScan("`  x@10`")
          .projection("cache[x.prop] as prop")
          .remoteBatchPropertiesWithFilter("cache[x.prop]")("x.prop > 20")
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime)
        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should return one node property columns when under Apply") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i % 2, "prop2" -> i * 2) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .|.remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]")("cache[x.prop1] = 0")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).filter(_ % 2 == 0).map(i => Array(0, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return one nullable property columns when under Apply and Optional") {
    givenGraph {

      tx.createNode(Label.label("START"))
      val s1 = tx.createNode(Label.label("START"))

      val e1 = tx.createNode()
      e1.setProperty("prop", 10)
      val e2 = tx.createNode()
      e2.setProperty("prop", 20)
      val e3 = tx.createNode()
      e3.setProperty("prop", 30)

      s1.createRelationshipTo(e1, RelationshipType.withName("R"))
      s1.createRelationshipTo(e2, RelationshipType.withName("R"))
      s1.createRelationshipTo(e3, RelationshipType.withName("R"))

    }

    val rowsWithNull: IndexedSeq[(String, Seq[Any])] = propertyPredicates.map {
      case (predicate, expected) if expected.isEmpty => (predicate, Seq(null, null))
      case (predicate, expected)                     => (predicate, expected ++ Seq(null))
    }

    val newTable = Table(("predicate", "expected"), rowsWithNull: _*)
    forEvery(newTable) { (predicate, expected) =>
      {

        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .apply()
          .|.optional("a")
          .|.projection("cache[x.prop] as prop")
          .|.remoteBatchPropertiesWithFilter("cache[x.prop]")(predicate)
          .|.expandAll("(s)-[]->(x)")
          .|.argument("s")
          .unwind("[1] as a")
          .nodeByLabelScan("s", "START")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> 20, "listParam" -> Array(10, 30)))

        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should return nullable property columns when under Apply and Optional") {
    givenGraph {
      val (nodes, _) = lineGraph(10, "L")
      var i = 0
      while (i < nodes.size) {
        nodes(i).setProperty("prop1", i % 2)
        nodes(i).setProperty("prop2", i * 2)
        i += 1
      }
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.optional("a")
      .|.projection("cache[y.prop1] as prop1", "cache[y.prop2] as prop2")
      .|.remoteBatchPropertiesWithFilter("cache[y.prop1]", "cache[y.prop2]")("y.prop1 = 1")
      .|.expandAll("(x)-[]->(y)")
      .|.argument("x")
      .unwind("[1] as a")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until 10).map(i =>
      i % 2 match {
        case 1 => Array(1, i * 2)
        case _ => Array[Any](null, null)
      }
    )
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return one relationship property columns when under Apply") {
    givenGraph {
      val (_, rels) = lineGraph(sizeHint, "R")
      rels.zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("prop1", i % 2)
          r.setProperty("prop2", i * 2)
      }
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.projection("cacheR[x.prop1] as prop1", "cacheR[x.prop2] as prop2")
      .|.remoteBatchPropertiesWithFilter("cacheR[x.prop1]", "cacheR[x.prop2]")("cacheR[x.prop1] = 0")
      .|.expandAll("(n)-[x:R]->()")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).filter(_ % 2 == 0).map(i => Array(0, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return nullable relationship property columns when under Apply and Optional") {
    givenGraph {
      val (_, rels) = lineGraph(10, "R")
      rels.zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("prop1", i % 2)
          r.setProperty("prop2", i * 2)
      }
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.optional("a")
      .|.projection("cacheR[x.prop1] as prop1", "cacheR[x.prop2] as prop2")
      .|.remoteBatchPropertiesWithFilter("cacheR[x.prop1]", "cacheR[x.prop2]")("x.prop1 = 0")
      .|.expandAll("(n)-[x:R]->()")
      .|.argument("n")
      .unwind("[1] as a")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until 10).map(i =>
      i % 2 match {
        case 0 => Array(0, i * 2)
        case _ => Array[Any](null, null)
      }
    )

    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return one node property columns when split into two plans under Apply with Sort") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 3 == 0 => Map("prop1" -> i, "prop2" -> i * 2)
          case i if i % 3 == 1 => Map("prop2" -> i * 2)
          case i if i % 4 == 2 => Map("prop1" -> i)
        }
      )
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.sort("prop1 ASC", "prop2 ASC")
      .|.projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .|.remoteBatchPropertiesWithFilter("cache[x.prop2]")("cache[x.prop2] IS NOT NULL")
      .|.remoteBatchPropertiesWithFilter("cache[x.prop1]")("cache[x.prop1] IS NOT NULL")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).filter(i => i % 3 == 0).map(i => Array(i, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should join on a remote batched property") {
    // given
    val nodeProperties = givenGraph {
      val nodes = nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
      nodes.map(_.getProperty("prop"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("aProp", "bProp")
      .projection("cache[a.prop] AS aProp", "cache[b.prop] AS bProp")
      .valueHashJoin("cache[a.prop]=cache[b.prop]")
      .|.remoteBatchPropertiesWithFilter("cache[b.prop]")("cache[b.prop] >= 10")
      .|.allNodeScan("b")
      .remoteBatchPropertiesWithFilter("cache[a.prop]")("cache[a.prop] < 20")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodeProperties.map(n => Array(n, n)).slice(10, 20)
    runtimeResult should beColumns("aProp", "bProp").withRows(expected)
  }

  test("should work with trail(repeat) single hop") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      var i = 0
      for (node <- nodes) {
        node.setProperty("foo", 42 + i % 4)
        i += 1
      }
    }

    val `(start) [(a)-[r]->(b)]{1,1} (end)` = TrailParameters(
      min = 1,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
      .|.remoteBatchPropertiesWithFilter("cache[b_inner.foo]")("cache[b_inner.foo] = 42")
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint / 4)
  }

  test("should work with trail(repeat) including zero repetition") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      var i = 0
      for (node <- nodes) {
        node.setProperty("foo", 42 + (i % 4))
        i += 1
      }
    }

    val `(start) [(a)-[r]->(b)]{0,1} (end)` = TrailParameters(
      min = 0,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .repeatTrail(`(start) [(a)-[r]->(b)]{0,1} (end)`)
      .|.remoteBatchPropertiesWithFilter("cache[b_inner.foo]")("cache[b_inner.foo] = 42")
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint + sizeHint / 4)
  }

  test("should work with trail(repeat) single hop, also on top") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      var i = 0
      for (node <- nodes) {
        node.setProperty("foo", 42 + i % 4)
        i += 1
      }
    }

    val `(start) [(a)-[r]->(b)]{1,1} (end)` = TrailParameters(
      min = 1,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .remoteBatchPropertiesWithFilter("cache[end.foo]")("cache[end.foo] = 42")
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
      .|.remoteBatchPropertiesWithFilter("cache[b_inner.foo]")("cache[b_inner.foo] = 42")
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint / 4)
  }

  test("should work with trail(repeat) under apply") {

    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      var i = 0
      for (node <- nodes) {
        node.setProperty("foo", 42 + i % 4)
        i += 1
      }
    }

    val `(start) [(a)-[r]->(b)]{1,1} (end)` = TrailParameters(
      min = 1,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.remoteBatchPropertiesWithFilter("cache[end.foo]")("cache[end.foo] = 42")
      .|.repeatTrail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
      .|.|.remoteBatchPropertiesWithFilter("cache[b_inner.foo]")("cache[b_inner.foo] = 42")
      .|.|.filter(isRepeatTrailUnique("r_inner"))
      .|.|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.|.argument("start", "a_inner")
      .|.allNodeScan("start")
      .unwind("[1,2,3,4] as u")
      .argument()
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount = sizeHint / 4
    runtimeResult should beColumns("c").withRows(singleColumn(Seq(
      expectedCount,
      expectedCount,
      expectedCount,
      expectedCount
    )))
  }

  test("should work with predicate on anonymous relationship variable and autoint parameter") {

    givenGraph {
      val (nodes, rels) = lineGraph(3, "R")
      nodes.zipWithIndex.foreach {
        case (n, i) => n.setProperty("prop", i)
      }
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("rel", i * 10)
      }
    }

    val generator = new AnonymousVariableNameGenerator()
    val anonymousVarName = generator.nextName

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop as prop")
      .remoteBatchProperties("cache[x.prop]")
      .apply()
      .|.limit(1)
      .|.remoteBatchPropertiesWithFilter(s"cacheR[`$anonymousVarName`.rel]")(
        s"`$anonymousVarName`.rel = $$`  AUTOINT0`"
      )
      .|.expandAll(s"(x)-[`$anonymousVarName`]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val result = execute(logicalQuery, runtime, Map("  AUTOINT0" -> 10))

    result should beColumns("prop").withSingleRow(1)
  }

  test("should escape property keys") {
    givenGraph {
      tx.createNode().setProperty("prop ` 1", 10)
      tx.createNode().setProperty("prop ` 1", 20)
      tx.createNode().setProperty("prop ` 1", 30)

    }

    val props = propertyPredicates.map {
      case (predicate, expected) => (predicate.replace("prop", "`prop `` 1`"), expected)
    }

    val newTable = Table(("expression", "expected"), props: _*)
    forEvery(newTable) { (predicate: String, expected) =>
      withClue(s"predicate: $predicate") {
        val query = new LogicalQueryBuilder(this)
          .produceResults("prop")
          .projection("cache[x.`prop `` 1`] as prop")
          .remoteBatchPropertiesWithFilter("cache[x.`prop `` 1`]")(predicate)
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime, Map[String, Any]("param" -> 20, "listParam" -> Array(10, 30)))
        result should beColumns("prop").withRows(singleColumn(expected))
      }
    }
  }

  test("should handle different cached-properties on lhs and rhs of cartesian product") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("aProp", "bProp")
      .projection("cache[a.prop] AS aProp", "cache[b.prop] AS bProp")
      .cartesianProduct()
      .|.remoteBatchPropertiesWithFilter("cache[b.prop]")("cache[b.prop] < 5")
      .|.allNodeScan("b")
      .remoteBatchPropertiesWithFilter("cache[a.prop]")("cache[a.prop] < 10")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      a <- 0 to 9
      b <- 0 to 4
    } yield Array(a, b)
    runtimeResult should beColumns("aProp", "bProp").withRows(expected)
  }

  test("should handle duplicated cached properties on rhs of nested cartesian product") {
    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("p", 10)
      n1.setProperty("p2", 11)
      val n2 = tx.createNode(Label.label("A"))
      n2.setProperty("p", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("p3", 30)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p3")
      .projection("cache[n3.p3] as p3")
      .remoteBatchProperties("cache[n3.p3]")
      .apply()
      .|.cartesianProduct()
      .|.|.cartesianProduct()
      .|.|.|.remoteBatchPropertiesWithFilter("cache[n1.p2]")("cache[n1.p2] = 11")
      .|.|.|.nodeByLabelScan("n3", "C", "n1")
      .|.|.remoteBatchProperties("cache[n1.p2]")
      .|.|.argument("n1")
      .|.remoteBatchPropertiesWithFilter("cache[n2.p]")("cache[n2.p] = 20")
      .|.allNodeScan("n2")
      .allNodeScan("n1")
      .build()

    val result = execute(query, runtime)

    result should beColumns("p3").withSingleRow(30)
  }

  test("should handle property access on parameters") {
    givenGraph {
      Seq("zero", "one", "two", "three").zipWithIndex.foreach {
        case (value1, i) =>
          val n = tx.createNode()
          n.setProperty("prop1", value1)
          n.setProperty("prop2", i)
          n.setProperty("name", s"node$i")
      }
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("name", "prop1", "prop2")
      .projection(
        "cache[n.name] as name",
        "cache[n.prop1] as prop1",
        "cache[n.prop2] as prop2"
      )
      .remoteBatchPropertiesWithFilter(
        "cache[n.name]",
        "cache[n.prop1]",
        "cache[n.prop2]"
      )(
        "n.prop1 = $paramMap.value1",
        "n.prop2 = $paramMap.value2"
      )
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime, Map("paramMap" -> Map[String, Any]("value1" -> "one", "value2" -> 1)))
    result should beColumns("name", "prop1", "prop2").withSingleRow("node1", "one", 1)
  }

  test("should push down argument from projection - node property") {
    givenGraph {
      nodePropertyGraph(
        10,
        {
          case i => Map("p" -> s"prop$i")
        }
      )
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("cacheN[n.p] AS p")
      .remoteBatchPropertiesWithFilter("cache[n.p]")(
        "cache[n.p] = prop"
      )
      .projection("'prop4' as prop")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime)

    result should beColumns("p").withSingleRow("prop4")
  }

  test("should push down argument from projection - relationship property") {
    givenGraph {
      val (_, relationships) = lineGraph(10, "REL")
      for (i <- relationships.indices) {
        relationships(i).setProperty("p", s"prop$i")

      }
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("cacheR[r.p] AS p")
      .remoteBatchPropertiesWithFilter("cacheR[r.p]")(
        "cacheR[r.p] = prop"
      )
      .projection("'prop4' as prop")
      .relationshipTypeScan("()-[r:REL]->()")
      .build()

    val result = execute(query, runtime)

    result should beColumns("p").withSingleRow("prop4")
  }

  test("should push down argument from cached node property") {
    val nodeCount = 10
    givenGraph {
      Seq("A", "B").foreach(label => {
        nodePropertyGraph(
          nodeCount,
          {
            case i => Map("p" -> i)
          },
          label
        )
      })
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("cache[a.p] as p1", "cache[b.p] as p2")
      .remoteBatchPropertiesWithFilter("cache[b.p]")("cache[b.p] = cache[a.p]")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B")
      .remoteBatchProperties("cache[a.p]")
      .nodeByLabelScan("a", "A")
      .build()

    val result = execute(query, runtime)

    val expected = (0 until nodeCount).map(i => Array(i, i))
    result should beColumns("p1", "p2").withRows(expected)
  }

  test("should push down argument from unwind") {
    givenGraph {
      nodePropertyGraph(
        10,
        {
          case i => Map("p" -> i)
        }
      )
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("cache[n.p] as p")
      .remoteBatchPropertiesWithFilter("cache[n.p]")("cache[n.p] = x")
      .unwind("[1,2,3] as x")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime)
    result should beColumns("p").withRows(singleColumn(Seq(1, 2, 3)))
  }

  test("should push down result of count aggregation ") {
    givenGraph {
      val aPersons = nodePropertyGraph(
        10,
        {
          case i => Map("i" -> i, "name" -> s"Ada$i")
        },
        "Person"
      )

      val bPersons = nodePropertyGraph(
        10,
        {
          case i => Map("i" -> i, "name" -> "Bob")
        },
        "Person"
      )

      for (i <- aPersons.indices) {
        val a = aPersons(i)
        val b = bPersons(i)
        var follows = 0
        while (follows < i) {
          a.createRelationshipTo(b, RelationshipType.withName("FOLLOWS"))
          follows += 1
        }
      }
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("name", "`a.i`")
      .projection("cache[a.name] as name", "cache[a.i] as `a.i`")
      .remoteBatchPropertiesWithFilter("cache[a.name]", "cache[a.i]")("follows_count = $expectedCount")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS follows_count"))
      .|.expandInto("(a)-[:FOLLOWS]->(b)")
      .|.argument("a", "b")
      .cartesianProduct()
      .|.nodeByLabelScan("a", "Person")
      .remoteBatchPropertiesWithFilter("cache[b.name]")("b.name = $personB")
      .nodeByLabelScan("b", "Person")
      .build()

    val result =
      execute(query, runtime, parameters = Map("personB" -> "Bob", "expectedCount" -> 2))

    result should beColumns("name", "a.i").withSingleRow("Ada2", 2)
  }

  test("should push down two arguments") {
    givenGraph {
      nodePropertyGraph(
        10,
        {
          case i => Map("p" -> i)
        }
      )
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("`n.p`")
      .projection("cache[n.p] as `n.p`")
      .remoteBatchPropertiesWithFilter("cache[n.p]")("cache[n.p] = arg1 OR cache[n.p] = arg2")
      .projection("5 as arg2")
      .unwind("[1,2,3] as arg1")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime)
    result should beColumns("n.p").withRows(singleColumn(Seq(1, 2, 3, 5, 5, 5)))
  }

  test("should push down three arguments") {
    val nodes = givenGraph {
      nodePropertyGraph(
        10,
        {
          case i => Map("p" -> i % 10)
        }
      )
    }

    val expected = nodes.map(_.getProperty("p").asInstanceOf[Int]).filter(i => i == 1 || i == 5 || i == 9)

    val query = new LogicalQueryBuilder(this)
      .produceResults("`n.p`")
      .projection("cache[n.p] as `n.p`")
      .remoteBatchPropertiesWithFilter("cache[n.p]")("cache[n.p] = arg1 OR cache[n.p] = arg2 OR cache[n.p] = arg3")
      .projection("1 as arg1", "5 as arg2", "9 as arg3")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime)
    result should beColumns("n.p").withRows(singleColumn(expected))
  }

  test("should push down argument on node property - on large graph") {
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("p" -> i % 10)
        }
      )
    }
    val expected = nodes.map(n => (n.getProperty("p").asInstanceOf[Integer], n.getElementId))
      .filter {
        case (p, _) => p == 1 || p == 2 || p == 3
      }.map {
        case (p, elementId) => Array(p, elementId)
      }

    val query = new LogicalQueryBuilder(this)
      .produceResults("`n.p`", "id")
      .projection("cache[n.p] as `n.p`", "elementId(n) as id")
      .remoteBatchPropertiesWithFilter("cache[n.p]")("cache[n.p] = arg1")
      .unwind("[1,2,3] as arg1")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime)

    result should beColumns("n.p", "id").withRows(expected)
  }

  test("should push down argument from exist") {
    givenGraph {
      val persons = nodePropertyGraph(
        5,
        {
          case i => Map("firstName" -> s"person$i")
        },
        "Person"
      )

      for (i <- persons.indices) {
        val person = persons(i)
        val dog = tx.createNode(Label.label("Dog"))

        if (i == 3) dog.setProperty("name", s"person$i")
        else dog.setProperty("name", s"dog$i")

        person.createRelationshipTo(dog, RelationshipType.withName("HAS_DOG"))
      }
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("name")
      .projection("cacheN[person.firstName] AS name")
      .semiApply()
      .|.remoteBatchPropertiesWithFilter("cacheN[dog.name]")(
        "cacheN[person.firstName] = dog.name"
      )
      .|.filter("dog:Dog")
      .|.expandAll("(person)-[:HAS_DOG]->(dog)")
      .|.argument("person")
      .remoteBatchProperties("cacheN[person.firstName]")
      .nodeByLabelScan("person", "Person", IndexOrderAscending)
      .build()

    val result = execute(query, runtime)
    result should beColumns("name").withSingleRow("person3")
  }

  test("should push down argument with trail(repeat) single hop - projection") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      var i = 0
      for (node <- nodes) {
        node.setProperty("foo", 42 + i % 4)
        i += 1
      }
    }

    val `(start) [(a)-[r]->(b)]{1,1} (end)` = TrailParameters(
      min = 1,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .projection("end.foo as c")
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
      .|.remoteBatchPropertiesWithFilter("cache[b_inner.foo]")("cache[b_inner.foo] = arg1")
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .projection("43 as arg1")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = Array.fill(sizeHint / 4)(43)
    // then
    runtimeResult should beColumns("c").withRows(singleColumn(expected))
  }

  test("should push down argument with trail(repeat) single hop - unwind") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      var i = 0
      for (node <- nodes) {
        node.setProperty("foo", 42 + i % 4)
        i += 1
      }
    }

    val `(start) [(a)-[r]->(b)]{1,1} (end)` = TrailParameters(
      min = 1,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .projection("end.foo as c")
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
      .|.remoteBatchPropertiesWithFilter("cache[b_inner.foo]")("cache[b_inner.foo] = arg1")
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .unwind("[0, 42, 43, 100] as arg1")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = Array.fill(sizeHint / 4)(42) ++ Array.fill(sizeHint / 4)(43)
    // then
    runtimeResult should beColumns("c").withRows(singleColumn(expected))
  }

  test("should push down arguments with trail(repeat) including zero repetition") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      var i = 0
      for (node <- nodes) {
        node.setProperty("foo", 42 + (i % 4))
        i += 1
      }
    }

    val `(start) [(a)-[r]->(b)]{0,1} (end)` = TrailParameters(
      min = 0,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .repeatTrail(`(start) [(a)-[r]->(b)]{0,1} (end)`)
      .|.remoteBatchPropertiesWithFilter("cache[b_inner.foo]")("cache[b_inner.foo] = arg1")
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .unwind("[42, 100] AS arg1")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint * 2 + sizeHint / 4)
  }

  test("should push down arguments on trail(repeat) under apply") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      var i = 0
      for (node <- nodes) {
        node.setProperty("foo", 42 + i % 4)
        i += 1
      }
    }

    val `(start) [(a)-[r]->(b)]{1,1} (end)` = TrailParameters(
      min = 1,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("`end.foo`")
      .apply()
      .|.projection("cache[end.foo] as `end.foo`")
      .|.remoteBatchPropertiesWithFilter("cache[end.foo]")("cache[end.foo] = arg1")
      .|.repeatTrail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
      .|.|.remoteBatchPropertiesWithFilter("cache[b_inner.foo]")("cache[b_inner.foo] = arg1")
      .|.|.filter(isRepeatTrailUnique("r_inner"))
      .|.|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.|.argument("start", "a_inner")
      .|.allNodeScan("start")
      .unwind("[1,2,42,43,10,45] as arg1")
      .argument()
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedRow = Array.fill(sizeHint / 4)(42) ++ Array.fill(sizeHint / 4)(43) ++ Array.fill(sizeHint / 4)(45)
    runtimeResult should beColumns("end.foo").withRows(singleColumn(expectedRow))
  }

  test("should push down combination of argument and query parameter") {
    givenGraph {
      nodePropertyGraph(
        10,
        {
          case i => Map("p" -> i)
        }
      )
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("cache[n.p] as p")
      .remoteBatchPropertiesWithFilter("cache[n.p]")("cache[n.p] = x + $param")
      .unwind("[1,2,3] as x")
      .allNodeScan("n")
      .build()

    val result = execute(query, runtime, Map[String, Any]("param" -> 2))
    result should beColumns("p").withRows(singleColumn(Seq(3, 4, 5)))
  }

  test("should push down argument under Apply") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i % 2, "prop2" -> i * 2) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .|.remoteBatchPropertiesWithFilter("cache[x.prop1]", "cache[x.prop2]")("cache[x.prop1] = arg1")
      .|.argument("x")
      .projection("0 as arg1")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).filter(_ % 2 == 0).map(i => Array(0, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should push down arguments with duplicated cached properties on rhs of nested cartesian product") {
    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("p", 10)
      n1.setProperty("p2", 11)
      val n2 = tx.createNode(Label.label("A"))
      n2.setProperty("p", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("p3", 30)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p3")
      .projection("cache[n3.p3] as p3")
      .remoteBatchProperties("cache[n3.p3]")
      .apply()
      .|.cartesianProduct()
      .|.|.cartesianProduct()
      .|.|.|.remoteBatchPropertiesWithFilter("cache[n1.p2]")("cache[n1.p2] = arg2")
      .|.|.|.nodeByLabelScan("n3", "C", "n1")
      .|.|.remoteBatchProperties("cache[n1.p2]")
      .|.|.argument("n1")
      .|.remoteBatchPropertiesWithFilter("cache[n2.p]")("cache[n2.p] = arg1")
      .|.allNodeScan("n2")
      .projection("20 as arg1", "11 as arg2")
      .allNodeScan("n1")
      .build()

    val result = execute(query, runtime)

    result should beColumns("p3").withSingleRow(30)
  }

  test("should push down predicate on node from collection") {

    givenGraph {
      val (nodes, _) = lineGraph(2, "R", "A")
      nodes.foreach(n => n.setProperty("property", "value"))
      nodeGraph(10)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("total")
      .aggregation(Seq.empty, Seq("count(node) as total"))
      .remoteBatchPropertiesWithFilter("cache[node.property]")("node.property IS NOT NULL", "node.property = $param")
      .filter("node:A")
      .unwind("nodes as node")
      .aggregation(Seq.empty, Seq("collect(a) as nodes"))
      .nodeByLabelScan("a", "A")
      .build()

    val result = execute(query, runtime, parameters = Map[String, Any]("param" -> "value"))

    result should beColumns("total").withSingleRow(2)
  }
}
