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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.Limited
import org.neo4j.cypher.internal.logical.plans.Unlimited
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.impl.coreapi.InternalTransaction

import java.util.Collections.emptyList

abstract class TrailTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should respect upper limit") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      (n1, n2, n3, n4, r12, r23, r34)
    }

    //(me:START) [(a)-[r]->(b)]{0,2} (you)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(min = 0,
             max = Limited(2),
             start = "me",
             end = Some("you"),
             innerStart = "a",
             innerEnd = "b",
             groupNodes = Set("a", "b"),
             groupRelationships = Set("r"),
             allRelationships = Set("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("me", "a")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
      )
    )
  }

  test("should handle missing end-node") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      (n1, n2, n3, n4, r12, r23, r34)
    }

    //(me:START) [(a)-[r]->(b)]{0,2}
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "a", "b", "r")
      .trail(min = 0,
        max = Limited(2),
        start = "me",
        end = None,
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set("a", "b"),
        groupRelationships = Set("r"),
        allRelationships = Set("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("me", "a")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "a", "b", "r").withRows(
      Seq(
        Array(n1, emptyList(), emptyList(), emptyList()),
        Array(n1, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
      )
    )
  }

  test("should respect lower limit") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      (n1, n2, n3, n4, r12, r23, r34)
    }

    //(me:START) [(a)-[r]->(b)]{2,2} (you)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(min = 2,
        max = Limited(2),
        start = "me",
        end = Some("you"),
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set("a", "b"),
        groupRelationships = Set("r"),
        allRelationships = Set("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("me", "a")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
      )
    )
  }

  test("should respect relationship uniqueness") {
    //given
    //          (n1)
    //        ↗     ↘
    //     (n4)     (n2)
    //        ↖     ↙
    //          (n3)
    val (n1, n2, n3, n4, r12, r23, r34, r41) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r41 = n4.createRelationshipTo(n1, RelationshipType.withName("R"))
      (n1, n2, n3, n4, r12, r23, r34, r41)
    }

    //(me:START) [(a)-[r]->(b)]{0, *} (you)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(min = 0,
        max = Unlimited,
        start = "me",
        end = Some("you"),
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set("a", "b"),
        groupRelationships = Set("r"),
        allRelationships = Set("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("me", "a")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n4, listOf(n1, n2, n3), listOf(n2, n3, n4), listOf(r12, r23, r34)),
        Array(n1, n1, listOf(n1, n2, n3, n4), listOf(n2, n3, n4, n1), listOf(r12, r23, r34, r41)),
      )
    )
  }

  test("should respect relationship uniqueness of several relationship variables") {
    //given
    //          (n1)
    //        ↗     ↘
    //      (n5)
    //        |
    //     (n4)     (n2)
    //        ↖     ↙
    //          (n3)
    val (n1, n2, n3, n4, n5, r12, r23, r34, r45, r51) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r45 = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r51 = n5.createRelationshipTo(n1, RelationshipType.withName("R"))
      (n1, n2, n3, n4, n5, r12, r23, r34, r45, r51)
    }

    //(me:START) [(a)-[r]->()-[]->(b)]{0, *} (you)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(min = 0,
        max = Unlimited,
        start = "me",
        end = Some("you"),
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set("a", "b"),
        groupRelationships = Set("r"),
        allRelationships = Set("r", "ranon"))
      .|.expandAll("(secret)-[ranon]->(b)")
      .|.expandAll("(a)-[r]->(secret)")
      .|.argument("me", "a")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n3, listOf(n1), listOf(n3), listOf(r12)),
        Array(n1, n5, listOf(n1, n3), listOf(n3, n5), listOf(r12, r34)),
      )
    )
  }

  test("should handle branched graph") {
    //      (n2) → (n4)
    //     ↗
    // (n1)
    //     ↘
    //      (n3) → (n5)
    val (n1, n2, n3, n4, n5, r12, r13, r24, r35) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r13 = n1.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r24 = n2.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r35 = n3.createRelationshipTo(n5, RelationshipType.withName("R"))
      (n1, n2, n3, n4, n5, r12, r13, r24, r35)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(min = 0,
        max = Limited(2),
        start = "me",
        end = Some("you"),
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set("a", "b"),
        groupRelationships = Set("r"),
        allRelationships = Set("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("me", "a")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),//0
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),//1
        Array(n1, n3, listOf(n1), listOf(n3), listOf(r13)),//1
        Array(n1, n4, listOf(n1, n2), listOf(n2, n4), listOf(r12, r24)),//2
        Array(n1, n5, listOf(n1, n3), listOf(n3, n5), listOf(r13, r35)),//2
      )
    )
  }

  test("should work for the zero length case") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      (n1, n2, n3, n4, r12, r23, r34)
    }

    //(me:START) [(a)-[r]->(b)]{0,0} (you)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(min = 0,
        max = Limited(0),
        start = "me",
        end = Some("you"),
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set("a", "b"),
        groupRelationships = Set("r"),
        allRelationships = Set("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("me", "a")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
      )
    )
  }

  test("should be able to reference LHS from RHS") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = given {
      val n1 = tx.createNode(label("START"))
      n1.setProperty("prop", 1)
      val n2 = tx.createNode()
      n2.setProperty("prop", 1)
      val n3 = tx.createNode()
      n3.setProperty("prop", 42)
      val n4 = tx.createNode()
      n4.setProperty("prop", 42)
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      (n1, n2, n3, n4, r12, r23, r34)
    }

    //(me:START) [(a)-[r]->(b) WHERE b.prop = me.prop]{0,2} (you)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(min = 0,
        max = Limited(2),
        start = "me",
        end = Some("you"),
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set("a", "b"),
        groupRelationships = Set("r"),
        allRelationships = Set("r"))
      .|.filter("b.prop = me.prop")
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("me", "a")
      .allNodeScan("me")
      .build()

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n2, n2, emptyList(), emptyList(), emptyList()),
        Array(n3, n3, emptyList(), emptyList(), emptyList()),
        Array(n3, n4, listOf(n3), listOf(n4), listOf(r34)),
        Array(n4, n4, emptyList(), emptyList(), emptyList()),
      )
    )
  }

  private def listOf(values: AnyRef*) = java.util.List.of(values:_*)
}


object TrailTestBase {
  /**
   *{{{
   *                ↗ (3) → (5:A) → (7:A)
   *   (1:A) → (2:A)   ↑
   *               ↘ (4:A) → (6:A) → (8:A) → (9:A) → (10:A)
   *}}}
   */
  def smallTestGraph(tx: InternalTransaction): (Node, Node, Node, Node, Node, Node, Node, Node, Node, Node) = {
    val n1 = tx.createNode(label("A"), label("START"))
    val n2 = tx.createNode(label("A"))
    val n3 = tx.createNode()//NOTE: no A
    val n4 = tx.createNode(label("A"))
    val n5 = tx.createNode(label("A"))
    val n6 = tx.createNode(label("A"))
    val n7 = tx.createNode(label("A"))
    val n8 = tx.createNode(label("A"))
    val n9 = tx.createNode(label("A"))
    val n10 = tx.createNode(label("A"))

    n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    n2.createRelationshipTo(n3, RelationshipType.withName("R"))
    n2.createRelationshipTo(n4, RelationshipType.withName("R"))
    n4.createRelationshipTo(n2, RelationshipType.withName("R"))
    n4.createRelationshipTo(n6, RelationshipType.withName("R"))
    n6.createRelationshipTo(n8, RelationshipType.withName("R"))
    n8.createRelationshipTo(n9, RelationshipType.withName("R"))
    n9.createRelationshipTo(n10, RelationshipType.withName("R"))
    n3.createRelationshipTo(n5, RelationshipType.withName("R"))
    n5.createRelationshipTo(n7, RelationshipType.withName("R"))
    (n1, n2, n3, n4, n5, n6, n7, n8, n9, n10)
  }
}


