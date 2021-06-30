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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.asScalaIteratorConverter

abstract class SubqueryForeachTestBase[CONTEXT <: RuntimeContext](
                                                         edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT],
                                                         sizeHint: Int
                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("subqueryForeach should forward the table from the LHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subqueryForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind(s"range(1, $sizeHint) AS x")
      .argument()
      .build(readOnly = false)

    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)

    // then
    val nodes = tx.getAllNodes.asScala.toList
    nodes.size
         .shouldBe(sizeHint)

    runtimeResult
      .should(beColumns("x")
        .withRows(singleColumn(Range.inclusive(1, sizeHint)))
        .withStatistics(nodesCreated = sizeHint, labelsAdded = sizeHint)
      )
  }

  test("subqueryForeach with empty LHS should do nothing") {
    val query = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .subqueryForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.argument()
      .unwind("[] AS x")
      .argument()
      .build(readOnly = false)

    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)

    // then
    val nodes = tx.getAllNodes.asScala.toList
    nodes.size
         .shouldBe(0)

    runtimeResult
      .should(beColumns().withNoRows())
  }

  test("subqueryForeach RHS should be able to read argument from LHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subqueryForeach()
      .|.emptyResult()
      .|.setProperty("n", "prop", "x")
      .|.create(createNode("n", "N"))
      .|.argument("x")
      .unwind(s"range(1, $sizeHint) AS x")
      .argument()
      .build(readOnly = false)

    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)

    // then
    val nodes = tx.getAllNodes.asScala.toList
    nodes.map(_.getProperty("prop").asInstanceOf[Long]).toSet
         .shouldEqual(Range.inclusive(1, sizeHint).toSet)

    runtimeResult
      .should(beColumns("x")
        .withRows(singleColumn(Range.inclusive(1, sizeHint)))
        .withStatistics(nodesCreated = sizeHint, labelsAdded = sizeHint, propertiesSet = sizeHint)
      )
  }


  test("subqueryForeach should handle nested subqueryForeach") {
    // UNWIND range(1, $sizeHint) AS x
    // CALL {
    //   WITH x
    //   CREATE (n:N)
    //   SET n.x = x
    //   UNWIND [1, 2] AS y
    //   CALL {
    //     WITH x, y
    //     CREATE (m:M)
    //     SET m.x = x
    //     SET m.y = y
    //   }
    // }
    // RETURN x
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subqueryForeach()
      .|.emptyResult()
      .|.subqueryForeach()
      .|.|.emptyResult()
      .|.|.setProperty("m", "y", "y")
      .|.|.setProperty("m", "x", "x")
      .|.|.create(createNode("m", "M"))
      .|.|.argument("x", "y")
      .|.unwind(s"[1, 2] AS y")
      .|.setProperty("n", "x", "x")
      .|.create(createNode("n", "N"))
      .|.argument("x")
      .unwind(s"range(1, $sizeHint) AS x")
      .argument()
      .build(readOnly = false)

    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)

    // then
    val ns = tx.findNodes(Label.label("N")).asScala.toList
    ns.size
      .shouldEqual(sizeHint)
    ns.map(_.getProperty("x").asInstanceOf[Long]).toSet
         .shouldEqual(Range.inclusive(1, sizeHint).toSet)

    val ms = tx.findNodes(Label.label("M")).asScala.toList
    ms.size
      .shouldEqual(sizeHint * 2)
    ms.map(_.getProperty("x").asInstanceOf[Long]).toSet
      .shouldEqual(Range.inclusive(1, sizeHint).toSet)
    ms.map(_.getProperty("y").asInstanceOf[Long]).toSet
      .shouldEqual(Set(1, 2))

    runtimeResult
      .should(beColumns("x")
        .withRows(singleColumn(Range.inclusive(1, sizeHint)))
        .withStatistics(nodesCreated = sizeHint * 3, labelsAdded = sizeHint * 3, propertiesSet = sizeHint * 5)
      )
  }

  test("subqueryForeach should handle nested apply") {
    // UNWIND range(1, $sizeHint) AS x
    // CALL {
    //   WITH x
    //   CREATE (n:N)
    //   SET n.x = x
    //   UNWIND [1, 2] AS y
    //   CALL {
    //     WITH y
    //     CREATE (m:M)
    //     SET m.y = y
    //     RETURN m
    //   }
    //   SET m.x = x
    // }
    // RETURN x
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subqueryForeach()
      .|.emptyResult()
      .|.setProperty("m", "x", "x")
      .|.apply(fromSubquery = true)
      .|.|.setProperty("m", "y", "y")
      .|.|.create(createNode("m", "M"))
      .|.|.argument("y")
      .|.unwind(s"[1, 2] AS y")
      .|.setProperty("n", "x", "x")
      .|.create(createNode("n", "N"))
      .|.argument("x")
      .unwind(s"range(1, $sizeHint) AS x")
      .argument()
      .build(readOnly = false)

    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)

    // then
    val ns = tx.findNodes(Label.label("N")).asScala.toList
    ns.size
      .shouldEqual(sizeHint)
    ns.map(_.getProperty("x").asInstanceOf[Long]).toSet
      .shouldEqual(Range.inclusive(1, sizeHint).toSet)

    val ms = tx.findNodes(Label.label("M")).asScala.toList
    ms.size
      .shouldEqual(sizeHint * 2)
    ms.map(_.getProperty("x").asInstanceOf[Long]).toSet
      .shouldEqual(Range.inclusive(1, sizeHint).toSet)
    ms.map(_.getProperty("y").asInstanceOf[Long]).toSet
      .shouldEqual(Set(1, 2))

    runtimeResult
      .should(beColumns("x")
        .withRows(singleColumn(Range.inclusive(1, sizeHint)))
        .withStatistics(nodesCreated = sizeHint * 3, labelsAdded = sizeHint * 3, propertiesSet = sizeHint * 5)
      )
  }

  test("subqueryForeach should handle being nested under apply") {
    // UNWIND range(1, $sizeHint) AS x
    // CALL {
    //   WITH x
    //   CREATE (n:N)
    //   UNWIND [1, 2] AS y
    //   CALL {
    //     WITH x, y
    //     CREATE (m:M)
    //     SET m.x = x
    //     SET m.y = y
    //   }
    //   RETURN n
    // }
    // SET n.x = x
    // RETURN x
    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .setProperty("n", "x", "x")
      .apply(fromSubquery = true)
      .|.subqueryForeach()
      .|.|.emptyResult()
      .|.|.setProperty("m", "y", "y")
      .|.|.setProperty("m", "x", "x")
      .|.|.create(createNode("m", "M"))
      .|.|.argument("x", "y")
      .|.unwind(s"[1, 2] AS y")
      .|.create(createNode("n", "N"))
      .|.argument("x")
      .unwind(s"range(1, $sizeHint) AS x")
      .argument()
      .build(readOnly = false)

    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)

    // then
    val ns = tx.findNodes(Label.label("N")).asScala.toList
    ns.size
      .shouldEqual(sizeHint)
    ns.map(_.getProperty("x").asInstanceOf[Long]).toSet
      .shouldEqual(Range.inclusive(1, sizeHint).toSet)

    val ms = tx.findNodes(Label.label("M")).asScala.toList
    ms.size
      .shouldEqual(sizeHint * 2)
    ms.map(_.getProperty("x").asInstanceOf[Long]).toSet
      .shouldEqual(Range.inclusive(1, sizeHint).toSet)
    ms.map(_.getProperty("y").asInstanceOf[Long]).toSet
      .shouldEqual(Set(1, 2))

    runtimeResult
      .should(beColumns("x")
        .withRows(singleColumn(Range.inclusive(1, sizeHint).flatMap(x => Seq(x, x))))
        .withStatistics(nodesCreated = sizeHint * 3, labelsAdded = sizeHint * 3, propertiesSet = sizeHint * 6)
      )
  }
}
