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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.internal.helpers.collection.Iterables

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

abstract class DeleteRelationshipTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("delete all relationships") {
    val relCount = sizeHint
    given {
      chainGraphs(1, (1 to relCount).map(_ => "KNOWS"): _*)
    }

    deleteAllTest(relCount)
  }

  test("delete all relationships on empty database") {
    deleteAllTest(relationshipCount = 0)
  }

  test("delete relationships with exhaustive limit") {
    given {
      chainGraphs(1, "KNOWS", "KNOWS", "KNOWS")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .exhaustiveLimit(1)
      .deleteRelationship("r")
      .relationshipTypeScan("()-[r:KNOWS]-()")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r").withStatistics(relationshipsDeleted = 3)
    Iterables.count(tx.getAllNodes) shouldBe 4
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  test("delete some relationships") {

    given {
      chainGraphs(2, "DELETE_ME", "SAVE_ME", "DELETE_ME", "SAVE_ME")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .deleteRelationship("r")
      .relationshipTypeScan("()-[r:DELETE_ME]-()")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r").withStatistics(relationshipsDeleted = 4)
    Iterables.count(tx.getAllRelationships) shouldBe 4
  }

  test("duplicate delete") {
    given {
      lollipopGraph()
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .deleteRelationship("r")
      .deleteRelationship("r")
      .relationshipTypeScan("()-[r:R]-()")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r").withStatistics(relationshipsDeleted = 3)
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  test("delete on rhs of apply") {
    given {
      chainGraphs(2, "FANCY", "FANCY", "FANCY")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.deleteRelationship("r")
      .|.argument("r")
      .relationshipTypeScan("()-[r:FANCY]-()")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r").withStatistics(relationshipsDeleted = 6)
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  test("create, delete and read relationship in the same tx - assure nice error message") {

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("t")
      .projection("type(r) AS t")
      .deleteRelationship("r")
      .create(createNode("n"), createNode("m"), createRelationship("r", "m", "R", "n"))
      .argument()
      .build(readOnly = false)

    // Same tx deletion of a relationship before reading its type isn't well defined and currently heavily implementation
    // dependant. No runtime throws with Block, and Legacy never throws. Here we ensure that we throw the correct
    // type of exception if we do throw.

    val expectedException = new EntityNotFoundException("Relationship with id 0 has been deleted in this transaction")

    Try(consume(execute(logicalQuery, runtime))) match {
      case Failure(exception) =>
        exception.getClass shouldBe expectedException.getClass
        exception.getMessage shouldBe expectedException.getMessage
      case Success(_) =>
    }
  }

  private def deleteAllTest(relationshipCount: Int): Unit = {

    val allRelationships = tx.getAllRelationships
    try {
      val relationshipsArray = allRelationships.iterator().asScala.toIndexedSeq.map(n => Array(n))

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .deleteRelationship("r")
        .expandAll("(n)-[r]->()")
        .allNodeScan("n")
        .build(readOnly = false)

      // then
      val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
      runtimeResult should beColumns("r")
        .withRows(relationshipsArray, listInAnyOrder = true)
        .withStatistics(relationshipsDeleted = relationshipCount)
    } finally {
      allRelationships.close()
    }
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }
}
