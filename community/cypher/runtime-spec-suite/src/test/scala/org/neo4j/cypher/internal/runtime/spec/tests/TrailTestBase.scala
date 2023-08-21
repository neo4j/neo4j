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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.GraphCreation.ComplexGraph
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(aa) ((e)<-[rrr]-(f)){1,}) (g)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(b) ((d)-[rr]->(aa:A) WHERE EXISTS {...} ){1,} (a)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me) ((b)-[r]->(c) WHERE EXISTS {...} ){1,} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me) [(a)-[r]->()-[]->(b)]{0,*} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me) [(a)-[r]->(b)]{0,*} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me) [(a)-[r]->(b)]{0,1} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me) [(a)-[r]->(b)]{0,2} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me) [(a)-[r]->(b)]{0,3} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me) [(a)-[r]->(b)]{1,2} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me) [(a)-[r]->(b)]{2,2} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(you) [(b)<-[r]-(a)]{0, *} (me)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(you) [(c)-[rr]->(d)]{0,1} (other)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(you) [(c)-[rr]->(d)]{0,2} (other)`
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase.`(you) [(c)-[rr]->(d)]{1,2} (other)`
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.values.virtual.VirtualValues.pathReference

import java.util
import java.util.Collections.emptyList

abstract class TrailTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should respect upper limit") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId))),
        Array(
          n1,
          n3,
          listOf(n1, n2),
          listOf(n2, n3),
          listOf(r12, r23),
          pathReference(Array(n1.getId, n2.getId, n3.getId), Array(r12.getId, r23.getId))
        )
      )
    ))
  }

  test("should handle unused anonymous end-node") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId))),
        Array(
          n1,
          listOf(n1, n2),
          listOf(n2, n3),
          listOf(r12, r23),
          pathReference(Array(n1.getId, n2.getId, n3.getId), Array(r12.getId, r23.getId))
        )
      )
    ))
  }

  test("should respect lower limit - when lower limit is same as upper limit") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{2,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(
          n1,
          n3,
          listOf(n1, n2),
          listOf(n2, n3),
          listOf(r12, r23),
          pathReference(Array(n1.getId, n2.getId, n3.getId), Array(r12.getId, r23.getId))
        )
      )
    ))
  }

  test("should respect lower limit - when lower limit is lower than upper limit") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{1,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId))),
        Array(
          n1,
          n3,
          listOf(n1, n2),
          listOf(n2, n3),
          listOf(r12, r23),
          pathReference(Array(n1.getId, n2.getId, n3.getId), Array(r12.getId, r23.getId))
        )
      )
    ))
  }

  test("should respect relationship uniqueness") {
    // given
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

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,*} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId))),
        Array(
          n1,
          n3,
          listOf(n1, n2),
          listOf(n2, n3),
          listOf(r12, r23),
          pathReference(Array(n1.getId, n2.getId, n3.getId), Array(r12.getId, r23.getId))
        ),
        Array(
          n1,
          n4,
          listOf(n1, n2, n3),
          listOf(n2, n3, n4),
          listOf(r12, r23, r34),
          pathReference(Array(n1.getId, n2.getId, n3.getId, n4.getId), Array(r12.getId, r23.getId, r34.getId))
        ),
        Array(
          n1,
          n1,
          listOf(n1, n2, n3, n4),
          listOf(n2, n3, n4, n1),
          listOf(r12, r23, r34, r41),
          pathReference(
            Array(n1.getId, n2.getId, n3.getId, n4.getId, n1.getId),
            Array(r12.getId, r23.getId, r34.getId, r41.getId)
          )
        )
      )
    ))
  }

  test("should respect relationship uniqueness when repeated relationship occurs at different depths - short") {
    // given
    //     (n1:START)
    //     ↓  ↑
    //     (n2)
    //     ↓  ↑
    //     (n3)

    val (n1, n2, n3, r12, r21, r23, r32) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r21 = n2.createRelationshipTo(n1, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r32 = n3.createRelationshipTo(n2, RelationshipType.withName("R"))
      (n1, n2, n3, r12, r21, r23, r32)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,*} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    /*
    n1
    n1-n2
    n1-n2-n1
    n1-n2-n3
    n1-n2-n1-n2     !filtered out!
    n1-n2-n3-n2
    n1-n2-n3-n2-n1
    n1-n2-n3-n2-n3  !filtered out!
     */

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId))),
        Array(
          n1,
          n1,
          listOf(n1, n2),
          listOf(n2, n1),
          listOf(r12, r21),
          pathReference(Array(n1.getId, n2.getId, n1.getId), Array(r12.getId, r21.getId))
        ),
        Array(
          n1,
          n3,
          listOf(n1, n2),
          listOf(n2, n3),
          listOf(r12, r23),
          pathReference(Array(n1.getId, n2.getId, n3.getId), Array(r12.getId, r23.getId))
        ),
        Array(
          n1,
          n2,
          listOf(n1, n2, n3),
          listOf(n2, n3, n2),
          listOf(r12, r23, r32),
          pathReference(Array(n1.getId, n2.getId, n3.getId, n2.getId), Array(r12.getId, r23.getId, r32.getId))
        ),
        Array(
          n1,
          n1,
          listOf(n1, n2, n3, n2),
          listOf(n2, n3, n2, n1),
          listOf(r12, r23, r32, r21),
          pathReference(
            Array(n1.getId, n2.getId, n3.getId, n2.getId, n1.getId),
            Array(r12.getId, r23.getId, r32.getId, r21.getId)
          )
        )
      )
    ))
  }

  test("should respect relationship uniqueness when repeated relationship occurs at different depths - long") {
    // given
    //      (n3)
    //      ↓ ↑
    //      (n2)
    //       ↑
    //                        ←
    //      (n1:START) → (n4) → (n5)
    //                        →
    //       ↓
    //      (n6)
    //      ↓ ↑
    //      (n7)

    val (n1, n2, n3, n4, n5, n6, n7, r12, r23, r32, r14, r45a, r45b, r54, r16, r67, r76) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()
      val n6 = tx.createNode()
      val n7 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r32 = n3.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r14 = n1.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r45a = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r45b = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r54 = n5.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r16 = n1.createRelationshipTo(n6, RelationshipType.withName("R"))
      val r67 = n6.createRelationshipTo(n7, RelationshipType.withName("R"))
      val r76 = n7.createRelationshipTo(n6, RelationshipType.withName("R"))
      (n1, n2, n3, n4, n5, n6, n7, r12, r23, r32, r14, r45a, r45b, r54, r16, r67, r76)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,*} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    /*
    n1

    n1-[r12]-n2
    n1-[r14]-n4
    n1-[r16]-n6

    n1-[r12]-n2-[r23]-n3
    n1-[r14]-n4-[r45a]-n5
    n1-[r14]-n4-[r45b]-n5
    n1-[r16]-n6-[r67]-n7

    n1-[r12]-n2-[r23]-n3-[r32]-n2
    n1-[r14]-n4-[r45a]-n5-[r54]-n4
    n1-[r14]-n4-[r45b]-n5-[r54]-n4
    n1-[r16]-n6-[r67]-n7-[r76]-n6

    n1-[r12]-n2-[r23]-n3-[r32]-n2-[r23]-n3              !filtered!
    n1-[r14]-n4-[r45a]-n5-[r54]-n4-[r45b]-n5
    n1-[r14]-n4-[r45b]-n5-[r54]-n4-[r45a]-n5
    n1-[r16]-n6-[r67]-n7-[r76]-n6-[r67]-n7              !filtered!

    n1-[r14]-n4-[r45a]-n5-[r54]-n4-[r45b]-n5-[r54]-n4   !filtered!
    n1-[r14]-n4-[r45b]-n5-[r54]-n4-[r45a]-n5-[r54]-n4   !filtered!
     */

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId))),
        Array(n1, n4, listOf(n1), listOf(n4), listOf(r14), pathReference(Array(n1.getId, n4.getId), Array(r14.getId))),
        Array(n1, n6, listOf(n1), listOf(n6), listOf(r16), pathReference(Array(n1.getId, n6.getId), Array(r16.getId))),
        Array(
          n1,
          n3,
          listOf(n1, n2),
          listOf(n2, n3),
          listOf(r12, r23),
          pathReference(Array(n1.getId, n2.getId, n3.getId), Array(r12.getId, r23.getId))
        ),
        Array(
          n1,
          n5,
          listOf(n1, n4),
          listOf(n4, n5),
          listOf(r14, r45a),
          pathReference(Array(n1.getId, n4.getId, n5.getId), Array(r14.getId, r45a.getId))
        ),
        Array(
          n1,
          n5,
          listOf(n1, n4),
          listOf(n4, n5),
          listOf(r14, r45b),
          pathReference(Array(n1.getId, n4.getId, n5.getId), Array(r14.getId, r45b.getId))
        ),
        Array(
          n1,
          n7,
          listOf(n1, n6),
          listOf(n6, n7),
          listOf(r16, r67),
          pathReference(Array(n1.getId, n6.getId, n7.getId), Array(r16.getId, r67.getId))
        ),
        Array(
          n1,
          n2,
          listOf(n1, n2, n3),
          listOf(n2, n3, n2),
          listOf(r12, r23, r32),
          pathReference(Array(n1.getId, n2.getId, n3.getId, n2.getId), Array(r12.getId, r23.getId, r32.getId))
        ),
        Array(
          n1,
          n4,
          listOf(n1, n4, n5),
          listOf(n4, n5, n4),
          listOf(r14, r45a, r54),
          pathReference(Array(n1.getId, n4.getId, n5.getId, n4.getId), Array(r14.getId, r45a.getId, r54.getId))
        ),
        Array(
          n1,
          n4,
          listOf(n1, n4, n5),
          listOf(n4, n5, n4),
          listOf(r14, r45b, r54),
          pathReference(Array(n1.getId, n4.getId, n5.getId, n4.getId), Array(r14.getId, r45b.getId, r54.getId))
        ),
        Array(
          n1,
          n6,
          listOf(n1, n6, n7),
          listOf(n6, n7, n6),
          listOf(r16, r67, r76),
          pathReference(Array(n1.getId, n6.getId, n7.getId, n6.getId), Array(r16.getId, r67.getId, r76.getId))
        ),
        Array(
          n1,
          n5,
          listOf(n1, n4, n5, n4),
          listOf(n4, n5, n4, n5),
          listOf(r14, r45a, r54, r45b),
          pathReference(
            Array(n1.getId, n4.getId, n5.getId, n4.getId, n5.getId),
            Array(r14.getId, r45a.getId, r54.getId, r45b.getId)
          )
        ),
        Array(
          n1,
          n5,
          listOf(n1, n4, n5, n4),
          listOf(n4, n5, n4, n5),
          listOf(r14, r45b, r54, r45a),
          pathReference(
            Array(n1.getId, n4.getId, n5.getId, n4.getId, n5.getId),
            Array(r14.getId, r45b.getId, r54.getId, r45a.getId)
          )
        )
      )
    ))
  }

  test("should respect relationship uniqueness of several relationship variables") {
    // given
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

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->()-[]->(b)]{0,*} (you)`)
      .|.filterExpressionOrString("not r_inner = ranon", isRepeatTrailUnique("ranon"))
      .|.expandAll("(secret)-[ranon]->(b_inner)")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(secret)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n3, listOf(n1), listOf(n3), listOf(r12)),
        Array(n1, n5, listOf(n1, n3), listOf(n3, n5), listOf(r12, r34))
      )
    ))
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
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])), // 0
        Array(
          n1,
          n2,
          listOf(n1),
          listOf(n2),
          listOf(r12),
          pathReference(Array(n1.getId, n2.getId), Array(r12.getId))
        ), // 1
        Array(
          n1,
          n3,
          listOf(n1),
          listOf(n3),
          listOf(r13),
          pathReference(Array(n1.getId, n3.getId), Array(r13.getId))
        ), // 1
        Array(
          n1,
          n4,
          listOf(n1, n2),
          listOf(n2, n4),
          listOf(r12, r24),
          pathReference(Array(n1.getId, n2.getId, n4.getId), Array(r12.getId, r24.getId))
        ), // 2
        Array(
          n1,
          n5,
          listOf(n1, n3),
          listOf(n3, n5),
          listOf(r13, r35),
          pathReference(Array(n1.getId, n3.getId, n5.getId), Array(r13.getId, r35.getId))
        ) // 2
      )
    ))
  }

  test("should be able to reference LHS from RHS") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = given {
      val n1 = tx.createNode()
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

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpressionOrString("b_inner.prop = me.prop", isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .allNodeScan("me")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId))),
        Array(n2, n2, emptyList(), emptyList(), emptyList(), pathReference(Array(n2.getId), Array.empty[Long])),
        Array(n3, n3, emptyList(), emptyList(), emptyList(), pathReference(Array(n3.getId), Array.empty[Long])),
        Array(n3, n4, listOf(n3), listOf(n4), listOf(r34), pathReference(Array(n3.getId, n4.getId), Array(r34.getId))),
        Array(n4, n4, emptyList(), emptyList(), emptyList(), pathReference(Array(n4.getId), Array.empty[Long]))
      )
    ))
  }

  test("should work when columns are introduced on top of trail") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "r2")
      .projection("r AS r2")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "r2").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(r12, r23))
      )
    ))
  }

  test("should work when concatenated") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    // given: (me:START) [(a)-[r]->(b)]{0,1} (you) [(c)-[rr]->(d)]{0,1} (other)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "other", "a", "b", "r", "c", "d", "rr")
      .projection(Map("path2" -> qppPath(varFor("you"), Seq(varFor("c"), varFor("rr")), varFor("other"))))
      .trail(`(you) [(c)-[rr]->(d)]{0,1} (other)`)
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("me", "you", "c_inner")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,1} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // 0: (n1)
    // 1: (n1) → (n2)

    // 0: (n1)
    // 0: (n1) → (n2)
    // 1: (n1) → (n2)
    // 1: (n1) → (n2) → (n3)

    // then
    runtimeResult should beColumns("me", "other", "a", "b", "r", "c", "d", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23))
      )
    ))
  }

  test("should respect relationship uniqueness when concatenated") {
    // given
    //          (n1)
    //        ↗     ↘
    //      (n3) <- (n2)
    val (n1, n2, n3, r12, r23, r31) = smallCircularGraph

    // given: (me:START) [(a)-[r]->(b)]{0,2} (you) [(c)-[rr]->(d)]{0,2} (other)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "other", "a", "b", "r", "c", "d", "rr")
      .projection(Map("path2" -> qppPath(varFor("you"), Seq(varFor("c"), varFor("rr")), varFor("other"))))
      .trail(`(you) [(c)-[rr]->(d)]{0,2} (other)`)
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("me", "c_inner")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // 0: (n1)
    // 1: (n1) → (n2)
    // 2: (n1) → (n2) → (n3)

    // 0: (n1)
    // 0: (n1) → (n2)
    // 0: (n1) → (n2) → (n3)
    // 1: (n1) → (n2)
    // 1: (n1) → (n2) → (n3)
    // 1: (n1) → (n2) → (n3) → (n1)
    // 2: (n1) → (n2) → (n3)
    // 2: (n1) → (n2) → (n3) → (n1)

    // then
    runtimeResult should beColumns("me", "other", "a", "b", "r", "c", "d", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
        Array(n1, n1, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n1), listOf(r23, r31)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
        Array(n1, n1, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n1), listOf(r31))
      )
    ))
  }

  test("should respect relationship uniqueness of previous relationships") {
    // given
    //          (n1)
    //        ↗     ↘
    //      (n3) <- (n2)
    val (n1, n2, n3, r12, r23, r31) = smallCircularGraph

    // given: MATCH (a:START)-[e]->(b) (()-[f]->(c))+
    // MATCH (a)-[e]->(b) (b)((anon_inner)-[f_inner]-(c_inner))+ (anon_end)

    val `(anon_start) (()-[f]->(c){1,*} (anon_end)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "anon_end",
      innerStart = "anon_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("c_inner", "c")),
      groupRelationships = Set(("f_inner", "f")),
      innerRelationships = Set("f_inner"),
      previouslyBoundRelationships = Set("e"),
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "e", "b", "f", "c")
      .trail(`(anon_start) (()-[f]->(c){1,*} (anon_end)`)
      .|.filterExpression(isRepeatTrailUnique("f_inner"))
      .|.expandAll("(anon_inner)-[f_inner]->(c_inner)")
      .|.argument("anon_inner")
      .filter("a:START")
      .allRelationshipsScan("(a)-[e]->(b)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a", "e", "b", "f", "c").withRows(inAnyOrder(
      Seq(
        Array(n1, r12, n2, listOf(r23), listOf(n3)),
        Array(n1, r12, n2, listOf(r23, r31), listOf(n3, n1))
      )
    ))
  }

  test("should produce rows with nullable slots") {

    // given: MATCH (me) OPTIONAL MATCH (me) [(a)-[r]->(b)]{0,*} (you:User) RETURN *

    val (n1, n2, n3, n4, _, _, _) = smallChainGraph
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b")
      .apply()
      .|.optional("me")
      .|.filter("you:User")
      .|.projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .|.trail(`(me) [(a)-[r]->(b)]{0,*} (you)`)
      .|.|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.|.argument("me", "a_inner")
      .|.argument("me")
      .allNodeScan("me")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("me", "you", "a", "b").withRows(inAnyOrder(
      Seq(
        Array(n1, null, null, null),
        Array(n2, null, null, null),
        Array(n3, null, null, null),
        Array(n4, null, null, null)
      )
    ))
  }

  test("should handle double relationship path with filter") {
    val (n1, n2, n3, r1, r2) = given {
      val n1 = tx.createNode()
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r1 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r2 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n1, n2, n3, r1, r2)
    }
    val `() ((a)->[r]->(b)->[s]->(c))+ ()` : TrailParameters = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_start",
      end = "anon_end",
      innerStart = "a_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("b_inner", "b"), ("c_inner", "c"), ("a_inner", "a")),
      groupRelationships = Set(("r_inner", "r"), ("s_inner", "s")),
      innerRelationships = Set("r_inner", "s_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c", "r", "s")
      .trail(`() ((a)->[r]->(b)->[s]->(c))+ ()`)
      .|.filterExpressionOrString("not s_inner = r_inner", isRepeatTrailUnique("s_inner"))
      .|.expandAll("(b_inner)-[s_inner]->(c_inner)")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("a", "anon_start")
      .allNodeScan("anon_start")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("a", "b", "c", "r", "s")
      .withRows(
        Seq(
          Array(listOf(n1), listOf(n2), listOf(n3), listOf(r1), listOf(r2))
        )
      )
  }

  test("should work with limit on top") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .limit(1)
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(rowCount(1))
  }

  test("should work with limit on rhs 1") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.unwind("[1] AS ignore") // pipelined specific: does not need a filtering morsel
      .|.nonFuseable() // pipelined specific: force break to test where RHS output receives normal Morsel but RHS leaf requires FilteringMorsel
      .|.limit(1)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
      )
    ))
  }

  test("should work with limit on rhs 2") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.limit(Int.MaxValue) // pipelined specific: test when RHS output receives FilteringMorsel & RHS leaf requires FilteringMorsel in different pipeline
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.nonFuseable()
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
      )
    ))
  }

  test("should work with filter on top") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .filter(s"id(you)<>${n2.getId}")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
      )
    ))
  }

  test("should work with filter on rhs 1") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpressionOrString(s"id(b_inner)<>${n3.getId}", isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12))
      )
    ))
  }

  test("should work with filter on rhs 2") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.unwind("[1] AS ignore") // pipelined specific: does not need a filtering morsel
      .|.nonFuseable() // pipelined specific: force break to test where RHS output receives normal Morsel but RHS leaf requires FilteringMorsel
      .|.filterExpressionOrString(s"id(b_inner)<>${n3.getId}", isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12))
      )
    ))
  }

  test("should work with union on RHS and (0,2) repetitions") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .projection(Map("path2" -> qppPath(varFor("you"), Seq(varFor("c"), varFor("rr")), varFor("other"))))
      .trail(`(you) [(c)-[rr]->(d)]{0,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.union()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.union()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n2, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
        Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
        Array(n1, n3, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
      )
    ))
  }

  test("should work with union on RHS and (1,2) repetitions") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .projection(Map("path2" -> qppPath(varFor("you"), Seq(varFor("c"), varFor("rr")), varFor("other"))))
      .trail(`(you) [(c)-[rr]->(d)]{1,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.union()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{1,2} (you)`)
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.union()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
        Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
        Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
      )
    ))
  }

  test("should work with cartesian product on RHS and (0,2) repetitions") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .projection(Map("path2" -> qppPath(varFor("you"), Seq(varFor("c"), varFor("rr")), varFor("other"))))
      .trail(`(you) [(c)-[rr]->(d)]{0,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.cartesianProduct()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.cartesianProduct()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n2, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
        Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
        Array(n1, n3, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
      )
    ))
  }

  test("should work with cartesian product on RHS and (1,2) repetitions") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .projection(Map("path2" -> qppPath(varFor("you"), Seq(varFor("c"), varFor("rr")), varFor("other"))))
      .trail(`(you) [(c)-[rr]->(d)]{1,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.cartesianProduct()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{1,2} (you)`)
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.cartesianProduct()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
        Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
        Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
      )
    ))
  }

  test("should work with join on RHS and (0,2) repetitions") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .projection(Map("path2" -> qppPath(varFor("you"), Seq(varFor("c"), varFor("rr")), varFor("other"))))
      .trail(`(you) [(c)-[rr]->(d)]{0,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.nodeHashJoin("d_inner")
      .|.|.allNodeScan("d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expand("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expand("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n2, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
        Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
        Array(n1, n3, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
      )
    ))
  }

  test("should work with join on RHS and (1,2) repetitions") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .projection(Map("path2" -> qppPath(varFor("you"), Seq(varFor("c"), varFor("rr")), varFor("other"))))
      .trail(`(you) [(c)-[rr]->(d)]{1,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.nodeHashJoin("d_inner")
      .|.|.allNodeScan("d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expand("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .trail(`(me) [(a)-[r]->(b)]{1,2} (you)`)
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expand("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
        Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
        Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
      )
    ))
  }

  test("complex case: chained trails") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .filter("end:LOOP")
      .apply()
      .|.projection(Map("path3" -> qppPath(varFor("middle"), Seq(varFor("c"), varFor("r2")), varFor("end"))))
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.filterExpressionOrString("d_inner:LOOP", isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .filter("middle:MIDDLE:LOOP")
      .projection(Map("path2" -> qppPath(varFor("firstMiddle"), Seq(varFor("a"), varFor("r1")), varFor("middle"))))
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.filterExpressionOrString("b_inner:MIDDLE", isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.filterExpressionOrString("anon_end_inner:MIDDLE", isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inAnyOrder(expectedResult)
    )
  }

  test("complex case: chained trails, solved with joins") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.filter("d_inner:LOOP")
      .|.|.nodeHashJoin("d_inner")
      .|.|.|.allNodeScan("d_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.filter("b_inner:MIDDLE")
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.nodeHashJoin("anon_end_inner")
      .|.|.filter("anon_end_inner:MIDDLE")
      .|.|.allNodeScan("anon_end_inner")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inAnyOrder(expectedResult)
    )
  }

  test("complex case: chained trails, solved with joins, plus excessive plan complexity") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .valueHashJoin("left=right")

      // Join RHS (identical to LHS)
      .|.projection("[start, middle, end, a, b, r1, c, d, r2] AS right")
      // insert distinct to "cancel out" the effect of unioning two identical inputs together
      .|.distinct(
        "start AS start",
        "middle AS middle",
        "end AS end",
        "a AS a",
        "b AS b",
        "r1 AS r1",
        "c AS c",
        "d AS d",
        "r2 AS r2"
      )
      .|.union()
      // (on RHS of Join) Union RHS (identical to Union LHS)
      .|.|.optional("end")
      .|.|.filter("end:LOOP")
      .|.|.apply()
      .|.|.|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.|.|.filter("r2_inner IS NOT NULL")
      .|.|.|.|.optional("middle")
      .|.|.|.|.filter("d_inner:LOOP")
      .|.|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.|.limit(Long.MaxValue)
      .|.|.|.|.|.allNodeScan("d_inner")
      .|.|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.|.argument("middle", "c_inner")
      .|.|.|.argument("middle")
      .|.|.filter("middle:MIDDLE:LOOP")
      .|.|.trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.|.|.filter("b_inner:MIDDLE")
      .|.|.|.nodeHashJoin("b_inner")
      .|.|.|.|.allNodeScan("b_inner")
      .|.|.|.limit(Long.MaxValue)
      .|.|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.|.optional("start")
      .|.|.|.argument("firstMiddle", "a_inner")
      .|.|.trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.|.allNodeScan("anon_end_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.|.argument("start", "anon_start_inner")
      .|.|.nodeByLabelScan("start", "START", IndexOrderNone)
      // (on RHS of Join) Union LHS (identical to Union RHS)
      .|.filter("end:LOOP")
      .|.apply()
      .|.|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.|.filter("d_inner:LOOP")
      .|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.allNodeScan("d_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.argument("middle", "c_inner")
      .|.|.argument("middle")
      .|.filter("middle:MIDDLE:LOOP")
      .|.trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.|.filter("b_inner:MIDDLE")
      .|.|.nodeHashJoin("b_inner")
      .|.|.|.allNodeScan("b_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.argument("firstMiddle", "a_inner")
      .|.trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.allNodeScan("anon_end_inner")
      .|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.argument("start", "anon_start_inner")
      .|.nodeByLabelScan("start", "START", IndexOrderNone)

      // Join LHS (identical to RHS)
      .projection("[start, middle, end, a, b, r1, c, d, r2] AS left")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.limit(Long.MaxValue)
      .|.|.filter("d_inner:LOOP")
      .|.|.nodeHashJoin("d_inner")
      .|.|.|.allNodeScan("d_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .limit(Long.MaxValue)
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.filter("b_inner:MIDDLE")
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.nodeHashJoin("anon_end_inner")
      .|.|.filter("anon_end_inner:MIDDLE")
      .|.|.allNodeScan("anon_end_inner")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inAnyOrder(expectedResult)
    )
  }

  test("should project original order of items in group variables when solved in reverse direction") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    // MATCH (me:START) ((a)-[r]->(b)){0,*} (you) when solved in reversed direction (right to left, expand b <- a),
    // items in group variables should be projected "left to right" as written in the query
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .filter("me:START")
      .trail(`(you) [(b)<-[r]-(a)]{0, *} (me)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(b_inner)<-[r_inner]-(a_inner)")
      .|.argument("you", "b_inner")
      .allNodeScan("you")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n4, listOf(n1, n2, n3), listOf(n2, n3, n4), listOf(r12, r23, r34))
      )
    ))
  }

  test("should respect relationship uniqueness between inner relationships") {

    // (n1:START) → (n2)
    val (n1, n2, r12) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      (n1, n2, r12)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "c", "r", "rr")
      .trail(TrailTestBase.`(me) [(a)-[r]->(b)<-[rr]-(c)]{0,1} (you)`)
      .|.filterExpressionOrString("not rr_inner = r_inner", isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(b_inner)<-[rr_inner]-(c_inner)")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("me", "you", "a", "b", "c", "r", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList())
      )
    ))
  }

  test("should respect relationship uniqueness between more inner relationships") {

    // (n1:START) → (n2) -> (n3)
    val (n1, n2, n3, r12, r23) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n1, n2, n3, r12, r23)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "c", "d", "r", "rr", "rrr")
      .trail(TrailTestBase.`(me) [(a)-[r]->(b)-[rr]->(c)<-[rrr]-(d)]{0,1} (you)`)
      .|.filterExpressionOrString(
        "not rrr_inner = r_inner",
        "not rrr_inner = rr_inner",
        isRepeatTrailUnique("rrr_inner")
      )
      .|.expandAll("(c_inner)<-[rrr_inner]-(d_inner)")
      .|.filterExpressionOrString("not rr_inner = r_inner", isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(b_inner)-[rr_inner]->(c_inner)")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("me", "you", "a", "b", "c", "d", "r", "rr", "rrr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList())
      )
    ))
  }

  test("should work with nested trails on rhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = given {
      val n1 = tx.createNode(label("A"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r21 = n2.createRelationshipTo(n1, withName("R"))
      val r23 = n2.createRelationshipTo(n3, withName("R"))
      (n1, n2, n3, r21, r23)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "b", "c", "r")
      .trail(`(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`)
      .|.apply()
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.trail(`(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`)
      .|.|.|.filter("aa_inner:A")
      .|.|.|.filterExpressionOrString(isRepeatTrailUnique("rr_inner"))
      .|.|.|.expandAll("(bb_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("bb_inner", "b_inner")
      .|.|.argument("b_inner")
      .|.filterExpressionOrString(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(b_inner)-[r_inner]->(c_inner)")
      .|.argument("b_inner")
      .allNodeScan("me")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "b", "c", "r").withRows(
      inAnyOrder(
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n2, n2, emptyList(), emptyList(), emptyList()),
          Array(n3, n3, emptyList(), emptyList(), emptyList()),
          Array(n2, n1, listOf(n2), listOf(n1), listOf(r21)),
          Array(n2, n3, listOf(n2), listOf(n3), listOf(r23))
        )
      )
    )
  }

  test("should work with multiple nested trails on rhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = given {
      val n1 = tx.createNode(label("A"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r21 = n2.createRelationshipTo(n1, withName("R"))
      val r23 = n2.createRelationshipTo(n3, withName("R"))
      (n1, n2, n3, r21, r23)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "b", "c", "r")
      .trail(`(me) ((b)-[r]->(c) WHERE EXISTS {...} ){1,} (you)`)
      .|.apply()
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.trail(`(b) ((d)-[rr]->(aa:A) WHERE EXISTS {...} ){1,} (a)`)
      .|.|.|.filter("aa_inner:A")
      .|.|.|.apply()
      .|.|.|.|.limit(1)
      .|.|.|.|.trail(`(aa) ((e)<-[rrr]-(f)){1,}) (g)`)
      .|.|.|.|.|.filterExpressionOrString(isRepeatTrailUnique("rrr_inner"))
      .|.|.|.|.|.expandAll("(e_inner)<-[rrr_inner]-(f_inner)")
      .|.|.|.|.|.argument("aa_inner", "e_inner")
      .|.|.|.|.argument("aa_inner")
      .|.|.|.filterExpressionOrString(isRepeatTrailUnique("rr_inner"))
      .|.|.|.expandAll("(d_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("b_inner", "d_inner")
      .|.|.argument("b_inner")
      .|.filterExpressionOrString(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(b_inner)-[r_inner]->(c_inner)")
      .|.argument("b_inner")
      .allNodeScan("me")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "b", "c", "r").withRows(
      inAnyOrder(
        Seq(
          Array(n2, n1, listOf(n2), listOf(n1), listOf(r21)),
          Array(n2, n3, listOf(n2), listOf(n3), listOf(r23))
        )
      )
    )

  }

  test("should project optional path as null") {

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .optional()
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withSingleRow(null)
  }

  test("should project optional path with value") {
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .optional()
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(
      inAnyOrder(
        Seq(
          Array(pathReference(Array(n1.getId), Array.empty[Long])),
          Array(pathReference(Array(n1.getId, n2.getId), Array(r12.getId))),
          Array(pathReference(Array(n1.getId, n2.getId, n3.getId), Array(r12.getId, r23.getId)))
        )
      )
    )
  }

  test("handle limit with trail as argument") {
    given(complexGraph())

    val plan0 = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2", "iteration")
      .apply()
      .|.valueHashJoin("left = right")
      .|.|.projection("[start, middle, end, a, b, r1, c, d, r2] AS right")
      .|.|.distinct(
        "a AS a",
        "middle AS middle",
        "b AS b",
        "r1 AS r1",
        "end AS end",
        "start AS start",
        "d AS d",
        "r2 AS r2",
        "c AS c"
      )
      .|.|.union()
      .|.|.|.optional("end")
      .|.|.|.filter("end:LOOP")
      .|.|.|.apply()
      .|.|.|.|.trail(TrailParameters(
        0,
        Unlimited,
        "middle",
        "end",
        "c_inner",
        "d_inner",
        Set(("c_inner", "c"), ("d_inner", "d")),
        Set(("r2_inner", "r2")),
        Set("r2_inner"),
        Set(),
        Set("r1"),
        false
      ))
    val plan1 = plan0.|.|.|.|.|.filter("r2_inner IS NOT NULL")
      .|.|.|.|.|.optional("middle")
      .|.|.|.|.|.filter("true")
      .|.|.|.|.|.filter("d_inner:LOOP")
      .|.|.|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.|.|.limit(9223372036854775807L) // We used to fail here
      .|.|.|.|.|.|.allNodeScan("d_inner")
      .|.|.|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.|.|.argument("middle", "c_inner")
      .|.|.|.|.argument("middle")
      .|.|.|.filter("true")
      .|.|.|.sort("foo ASC")
      .|.|.|.projection("start.foo AS foo")
      .|.|.|.filter("middle:MIDDLE AND middle:LOOP")
      .|.|.|.trail(TrailParameters(
        0,
        Unlimited,
        "firstMiddle",
        "middle",
        "a_inner",
        "b_inner",
        Set(("a_inner", "a"), ("b_inner", "b")),
        Set(("r1_inner", "r1")),
        Set("r1_inner"),
        Set(),
        Set(),
        false
      ))
      .|.|.|.|.filter("b_inner:MIDDLE")
      .|.|.|.|.nodeHashJoin("b_inner")
      .|.|.|.|.|.allNodeScan("b_inner")
      .|.|.|.|.limit(9223372036854775807L)
      .|.|.|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.|.|.optional("start")
      .|.|.|.|.filter("true")
      .|.|.|.|.argument("firstMiddle", "a_inner")
      .|.|.|.trail(TrailParameters(
        1,
        Limited(1),
        "start",
        "firstMiddle",
        "anon_start_inner",
        "anon_end_inner",
        Set(),
        Set(),
        Set("anon_r_inner"),
        Set(),
        Set(),
        false
      ))
    val plan2 = plan1.|.|.|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.|.|.filter("true")
      .|.|.|.|.|.allNodeScan("anon_end_inner")
      .|.|.|.|.filter("true")
      .|.|.|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.|.|.argument("start", "anon_start_inner")
      .|.|.|.filter("true")
      .|.|.|.nodeByLabelScan("start", "START", IndexOrderNone)
      .|.|.filter("end:LOOP")
      .|.|.apply()
      .|.|.|.filter("true")
      .|.|.|.trail(TrailParameters(
        0,
        Unlimited,
        "middle",
        "end",
        "c_inner",
        "d_inner",
        Set(("c_inner", "c"), ("d_inner", "d")),
        Set(("r2_inner", "r2")),
        Set("r2_inner"),
        Set(),
        Set("r1"),
        false
      ))
      .|.|.|.|.filter("d_inner:LOOP")
      .|.|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.|.allNodeScan("d_inner")
      .|.|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.|.filter("true")
      .|.|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.|.argument("middle", "c_inner")
      .|.|.|.filter("true")
      .|.|.|.argument("middle")
      .|.|.sort("foo ASC")
      .|.|.filter("true")
      .|.|.projection("start.foo AS foo")
      .|.|.filter("true")
      .|.|.filter("middle:MIDDLE AND middle:LOOP")
      .|.|.trail(TrailParameters(
        0,
        Unlimited,
        "firstMiddle",
        "middle",
        "a_inner",
        "b_inner",
        Set(("a_inner", "a"), ("b_inner", "b")),
        Set(("r1_inner", "r1")),
        Set("r1_inner"),
        Set(),
        Set(),
        false
      ))
    val plan3 = plan2.|.|.|.filter("b_inner:MIDDLE")
      .|.|.|.nodeHashJoin("b_inner")
      .|.|.|.|.allNodeScan("b_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.|.filter("true")
      .|.|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.|.filter("true")
      .|.|.|.argument("firstMiddle", "a_inner")
      .|.|.trail(TrailParameters(
        1,
        Limited(1),
        "start",
        "firstMiddle",
        "anon_start_inner",
        "anon_end_inner",
        Set(),
        Set(),
        Set("anon_r_inner"),
        Set(),
        Set(),
        false
      ))
      .|.|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.|.filter("true")
      .|.|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.|.allNodeScan("anon_end_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.|.filter("true")
      .|.|.|.argument("start", "anon_start_inner")
      .|.|.filter("true")
      .|.|.nodeByLabelScan("start", "START", IndexOrderNone)
      .|.projection("[start, middle, end, a, b, r1, c, d, r2] AS left")
      .|.filter("end:LOOP")
      .|.apply()
      .|.|.trail(TrailParameters(
        0,
        Unlimited,
        "middle",
        "end",
        "c_inner",
        "d_inner",
        Set(("c_inner", "c"), ("d_inner", "d")),
        Set(("r2_inner", "r2")),
        Set("r2_inner"),
        Set(),
        Set("r1"),
        false
      ))
      .|.|.|.filter("true")
      .|.|.|.limit(9223372036854775807L)
      .|.|.|.filter("d_inner:LOOP")
      .|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.allNodeScan("d_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.filter("true")
      .|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.filter("true")
      .|.|.|.argument("middle", "c_inner")
      .|.|.argument("middle")
      .|.limit(9223372036854775807L)
      .|.sort("foo ASC")
      .|.filter("true")
      .|.projection("start.foo AS foo")
      .|.filter("middle:MIDDLE AND middle:LOOP")
      .|.trail(TrailParameters(
        0,
        Unlimited,
        "firstMiddle",
        "middle",
        "a_inner",
        "b_inner",
        Set(("a_inner", "a"), ("b_inner", "b")),
        Set(("r1_inner", "r1")),
        Set("r1_inner"),
        Set(),
        Set(),
        false
      ))
    val plan = plan3.|.|.filter("true")
      .|.|.filter("b_inner:MIDDLE")
      .|.|.nodeHashJoin("b_inner")
      .|.|.|.allNodeScan("b_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.argument("firstMiddle", "a_inner")
      .|.filter("true")
      .|.trail(TrailParameters(
        1,
        Limited(1),
        "start",
        "firstMiddle",
        "anon_start_inner",
        "anon_end_inner",
        Set(),
        Set(),
        Set("anon_r_inner"),
        Set(),
        Set(),
        false
      ))
      .|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.filter("true")
      .|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.allNodeScan("anon_end_inner")
      .|.|.filter("true")
      .|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.argument("start", "anon_start_inner")
      .|.nodeByLabelScan("start", "START", IndexOrderNone)
      .unwind("range(1, 4) AS iteration")
      .argument()
      .build()

    // Then there should be no exceptions
    execute(plan, runtime).awaitAll()
  }

  protected def listOf(values: AnyRef*): util.List[AnyRef] = TrailTestBase.listOf(values: _*)

  //  (n0:START)                                                  (n6:LOOP)
  //             ↘             →                                ↗     |
  //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
  //             ↗             ←                                ↖     ↓
  //  (n2:START)                                                  (n7:LOOP)
  protected def givenComplexGraph(): ComplexGraph = {
    given(complexGraph())
  }

  /**
   * NOTE: Expected result obviously assumes that certain (equivalent) plans are used, which is the case for all tests calling this method.
   *
   * Those tests all use plans that solve the following the trail configurations:
   *  1. (start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)
   *  2. (firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)
   *  3. (middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)
   *
   * And return result columns: "start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2"
   *
   * The input graph must be [[complexGraph]], i.e.:
   *
   * (n0:START)                                                  (n6:LOOP)
   *            ↘             →                                ↗     |
   * (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
   *            ↗             ←                                ↖     ↓
   * (n2:START)                                                  (n7:LOOP)
   *
   */
  protected def complexGraphAndExpectedResult: Seq[Array[Object]] = {
    val (n0, n1, n2, n3, n4, n5, n6, n7, r03, r13, r23, r34a, r34b, r43, r45, r56, r67, r75) =
      ComplexGraph.unapply(givenComplexGraph).get

    Seq(
      Array(n0, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), emptyList(), emptyList(), emptyList()),
      Array(n0, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), listOf(n5), listOf(n6), listOf(r56)),
      Array(
        n0,
        n3,
        n5,
        n7,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34a, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n0,
        n3,
        n5,
        n5,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34a, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(n0, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), emptyList(), emptyList(), emptyList()),
      Array(n0, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), listOf(n5), listOf(n6), listOf(r56)),
      Array(
        n0,
        n3,
        n5,
        n7,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34b, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n0,
        n3,
        n5,
        n5,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34b, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(
        n0,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        emptyList(),
        emptyList(),
        emptyList()
      ),
      Array(
        n0,
        n3,
        n5,
        n6,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5),
        listOf(n6),
        listOf(r56)
      ),
      Array(
        n0,
        n3,
        n5,
        n7,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n0,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(
        n0,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        emptyList(),
        emptyList(),
        emptyList()
      ),
      Array(
        n0,
        n3,
        n5,
        n6,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5),
        listOf(n6),
        listOf(r56)
      ),
      Array(
        n0,
        n3,
        n5,
        n7,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n0,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),

      // -----

      Array(n1, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), emptyList(), emptyList(), emptyList()),
      Array(n1, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), listOf(n5), listOf(n6), listOf(r56)),
      Array(
        n1,
        n3,
        n5,
        n7,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34a, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n1,
        n3,
        n5,
        n5,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34a, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(n1, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), emptyList(), emptyList(), emptyList()),
      Array(n1, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), listOf(n5), listOf(n6), listOf(r56)),
      Array(
        n1,
        n3,
        n5,
        n7,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34b, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n1,
        n3,
        n5,
        n5,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34b, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(
        n1,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        emptyList(),
        emptyList(),
        emptyList()
      ),
      Array(
        n1,
        n3,
        n5,
        n6,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5),
        listOf(n6),
        listOf(r56)
      ),
      Array(
        n1,
        n3,
        n5,
        n7,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n1,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(
        n1,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        emptyList(),
        emptyList(),
        emptyList()
      ),
      Array(
        n1,
        n3,
        n5,
        n6,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5),
        listOf(n6),
        listOf(r56)
      ),
      Array(
        n1,
        n3,
        n5,
        n7,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n1,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),

      // -----

      Array(n2, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), emptyList(), emptyList(), emptyList()),
      Array(n2, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), listOf(n5), listOf(n6), listOf(r56)),
      Array(
        n2,
        n3,
        n5,
        n7,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34a, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n2,
        n3,
        n5,
        n5,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34a, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(n2, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), emptyList(), emptyList(), emptyList()),
      Array(n2, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), listOf(n5), listOf(n6), listOf(r56)),
      Array(
        n2,
        n3,
        n5,
        n7,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34b, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n2,
        n3,
        n5,
        n5,
        listOf(n3, n4),
        listOf(n4, n5),
        listOf(r34b, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(
        n2,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        emptyList(),
        emptyList(),
        emptyList()
      ),
      Array(
        n2,
        n3,
        n5,
        n6,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5),
        listOf(n6),
        listOf(r56)
      ),
      Array(
        n2,
        n3,
        n5,
        n7,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n2,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34a, r43, r34b, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      ),
      Array(
        n2,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        emptyList(),
        emptyList(),
        emptyList()
      ),
      Array(
        n2,
        n3,
        n5,
        n6,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5),
        listOf(n6),
        listOf(r56)
      ),
      Array(
        n2,
        n3,
        n5,
        n7,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5, n6),
        listOf(n6, n7),
        listOf(r56, r67)
      ),
      Array(
        n2,
        n3,
        n5,
        n5,
        listOf(n3, n4, n3, n4),
        listOf(n4, n3, n4, n5),
        listOf(r34b, r43, r34a, r45),
        listOf(n5, n6, n7),
        listOf(n6, n7, n5),
        listOf(r56, r67, r75)
      )
    )
  }

  // (n1) → (n2) → (n3) → (n4)
  protected def smallChainGraph: (Node, Node, Node, Node, Relationship, Relationship, Relationship) = {
    given {
      val chain = chainGraphs(1, "R", "R", "R").head
      (
        chain.nodeAt(0),
        chain.nodeAt(1),
        chain.nodeAt(2),
        chain.nodeAt(3),
        chain.relationshipAt(0),
        chain.relationshipAt(1),
        chain.relationshipAt(2)
      )
    }
  }

  //          (n1)
  //        ↗     ↘
  //      (n3) <- (n2)
  protected def smallCircularGraph: (Node, Node, Node, Relationship, Relationship, Relationship) = {
    given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r31 = n3.createRelationshipTo(n1, RelationshipType.withName("R"))
      (n1, n2, n3, r12, r23, r31)
    }
  }
}

object TrailTestBase {
  def listOf(values: AnyRef*): util.List[AnyRef] = java.util.List.of[AnyRef](values: _*)

  private def createMeYouTrailParameters(min: Int, max: UpperBound): TrailParameters = {
    TrailParameters(
      min,
      max,
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )
  }

  private def createYouOtherTrailParameters(min: Int, max: UpperBound): TrailParameters = {
    TrailParameters(
      min,
      max,
      start = "you",
      end = "other",
      innerStart = "c_inner",
      innerEnd = "d_inner",
      groupNodes = Set(("c_inner", "c"), ("d_inner", "d")),
      groupRelationships = Set(("rr_inner", "rr")),
      innerRelationships = Set("rr_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r"),
      reverseGroupVariableProjections = false
    )
  }

  val `(me) [(a)-[r]->(b)]{0,1} (you)` : TrailParameters =
    createMeYouTrailParameters(min = 0, max = Limited(1))

  val `(me) [(a)-[r]->(b)]{0,2} (you)` : TrailParameters =
    createMeYouTrailParameters(min = 0, max = Limited(2))

  val `(me) [(a)-[r]->(b)]{0,3} (you)` : TrailParameters =
    createMeYouTrailParameters(min = 0, max = Limited(3))

  val `(me) [(a)-[r]->(b)]{0,*} (you)` : TrailParameters =
    createMeYouTrailParameters(min = 0, max = Unlimited)

  val `(me) [(a)-[r]->(b)]{1,1} (you)` : TrailParameters =
    createMeYouTrailParameters(min = 1, max = Limited(1))

  val `(me) [(a)-[r]->(b)]{1,2} (you)` : TrailParameters =
    createMeYouTrailParameters(min = 1, max = Limited(2))

  val `(me) [(a)-[r]->(b)]{2,2} (you)` : TrailParameters =
    createMeYouTrailParameters(min = 2, max = Limited(2))

  val `(you) [(c)-[rr]->(d)]{0,1} (other)` : TrailParameters =
    createYouOtherTrailParameters(min = 0, max = Limited(1))

  val `(you) [(c)-[rr]->(d)]{0,2} (other)` : TrailParameters =
    createYouOtherTrailParameters(min = 0, max = Limited(2))

  val `(you) [(c)-[rr]->(d)]{1,2} (other)` : TrailParameters =
    createYouOtherTrailParameters(min = 1, max = Limited(2))

  val `(me) [(a)-[r]->()-[]->(b)]{0,*} (you)` : TrailParameters = TrailParameters(
    min = 0,
    max = Unlimited,
    start = "me",
    end = "you",
    innerStart = "a_inner",
    innerEnd = "b_inner",
    groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
    groupRelationships = Set(("r_inner", "r")),
    innerRelationships = Set("r_inner", "ranon"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty,
    reverseGroupVariableProjections = false
  )

  val `(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)` : TrailParameters = TrailParameters(
    min = 1,
    max = Limited(1),
    start = "start",
    end = "firstMiddle",
    innerStart = "anon_start_inner",
    innerEnd = "anon_end_inner",
    groupNodes = Set(),
    groupRelationships = Set(),
    innerRelationships = Set("anon_r_inner"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty,
    reverseGroupVariableProjections = false
  )

  val `(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)` : TrailParameters = TrailParameters(
    min = 0,
    max = Unlimited,
    start = "firstMiddle",
    end = "middle",
    innerStart = "a_inner",
    innerEnd = "b_inner",
    groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
    groupRelationships = Set(("r1_inner", "r1")),
    innerRelationships = Set("r1_inner"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty,
    reverseGroupVariableProjections = false
  )

  val `(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)` : TrailParameters = TrailParameters(
    min = 0,
    max = Unlimited,
    start = "middle",
    end = "end",
    innerStart = "c_inner",
    innerEnd = "d_inner",
    groupNodes = Set(("c_inner", "c"), ("d_inner", "d")),
    groupRelationships = Set(("r2_inner", "r2")),
    innerRelationships = Set("r2_inner"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set("r1"),
    reverseGroupVariableProjections = false
  )

  val `(you) [(b)<-[r]-(a)]{0, *} (me)` : TrailParameters =
    TrailParameters(
      min = 0,
      max = Unlimited,
      start = "you",
      end = "me",
      innerStart = "b_inner",
      innerEnd = "a_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

  val `(me) [(a)-[r]->(b)<-[rr]-(c)]{0,1} (you)` : TrailParameters =
    TrailParameters(
      min = 0,
      max = UpperBound.Limited(1),
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c")),
      groupRelationships = Set(("r_inner", "r"), ("rr_inner", "rr")),
      innerRelationships = Set("r_inner", "rr_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

  val `(me) ((b)-[r]->(c) WHERE EXISTS {...} ){1,} (you)` : TrailParameters = TrailParameters(
    1,
    UpperBound.Unlimited,
    "me",
    "you",
    "b_inner",
    "c_inner",
    Set(("b_inner", "b"), ("c_inner", "c")),
    Set(("r_inner", "r")),
    Set("r_inner"),
    Set(),
    Set(),
    false
  )

  val `(b) ((d)-[rr]->(aa:A) WHERE EXISTS {...} ){1,} (a)` : TrailParameters = TrailParameters(
    1,
    UpperBound.Unlimited,
    "b_inner",
    "a",
    "d_inner",
    "aa_inner",
    Set(("d_inner", "d"), ("aa_inner", "aa")),
    Set(("rr_inner", "rr")),
    Set("rr_inner"),
    Set(),
    Set(),
    false
  )

  val `(aa) ((e)<-[rrr]-(f)){1,}) (g)` : TrailParameters = TrailParameters(
    1,
    UpperBound.Unlimited,
    "aa_inner",
    "g",
    "e_inner",
    "f_inner",
    Set(("e_inner", "e"), ("f_inner", "f")),
    Set(("rrr_inner", "rrr")),
    Set("rrr_inner"),
    Set(),
    Set(),
    false
  )

  val `(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)` : TrailParameters =
    TrailParameters(
      min = 0,
      max = UpperBound.Unlimited,
      start = "me",
      end = "you",
      innerStart = "b_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("b_inner", "b"), ("c_inner", "c")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

  val `(me) [(a)-[r]->(b)-[rr]->(c)<-[rrr]-(d)]{0,1} (you)` : TrailParameters =
    TrailParameters(
      min = 0,
      max = UpperBound.Limited(1),
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "d_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
      groupRelationships = Set(("r_inner", "r"), ("rr_inner", "rr"), ("rrr_inner", "rrr")),
      innerRelationships = Set("r_inner", "rr_inner", "rrr_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

  val `(b_inner)((bb)-[rr]->(aa:A)){0,}(a)` : TrailParameters = TrailParameters(
    min = 0,
    max = UpperBound.Unlimited,
    start = "b_inner",
    end = "a",
    innerStart = "bb_inner",
    innerEnd = "aa_inner",
    groupNodes = Set(("bb_inner", "bb"), ("aa_inner", "aa")),
    groupRelationships = Set(("rr_inner", "rr")),
    innerRelationships = Set("rr_inner"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty,
    reverseGroupVariableProjections = false
  )
}

trait OrderedTrailTestBase[CONTEXT <: RuntimeContext] {
  self: TrailTestBase[CONTEXT] =>

  test("should work with multiple nested trails - with leveraged order on lhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = smallTreeGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "b", "c", "r")
      .trail(`(me) ((b)-[r]->(c) WHERE EXISTS {...} ){1,} (you)`).withLeveragedOrder()
      .|.apply()
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.trail(`(b) ((d)-[rr]->(aa:A) WHERE EXISTS {...} ){1,} (a)`).withLeveragedOrder()
      .|.|.|.filter("aa_inner:A")
      .|.|.|.apply()
      .|.|.|.|.limit(1)
      .|.|.|.|.trail(`(aa) ((e)<-[rrr]-(f)){1,}) (g)`).withLeveragedOrder()
      .|.|.|.|.|.filterExpressionOrString(isRepeatTrailUnique("rrr_inner"))
      .|.|.|.|.|.expandAll("(e_inner)<-[rrr_inner]-(f_inner)")
      .|.|.|.|.|.argument("aa_inner", "e_inner")
      .|.|.|.|.argument("aa_inner")
      .|.|.|.filterExpressionOrString(isRepeatTrailUnique("rr_inner"))
      .|.|.|.expandAll("(d_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("b_inner", "d_inner")
      .|.|.argument("b_inner")
      .|.filterExpressionOrString(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(b_inner)-[r_inner]->(c_inner)")
      .|.argument("b_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .allNodeScan("me")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "b", "c", "r").withRows(
      inPartialOrder(
        Seq(Seq(
          Array(n2, n3, listOf(n2), listOf(n3), listOf(r23)),
          Array(n2, n1, listOf(n2), listOf(n1), listOf(r21))
        ))
      )
    )
  }

  test("should work with nested trails on rhs - with leveraged order on lhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = smallTreeGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "b", "c", "r")
      .trail(`(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`).withLeveragedOrder()
      .|.apply()
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.trail(`(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`)
      .|.|.|.filter("aa_inner:A")
      .|.|.|.filterExpressionOrString(isRepeatTrailUnique("rr_inner"))
      .|.|.|.expandAll("(bb_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("bb_inner", "b_inner")
      .|.|.argument("b_inner")
      .|.filterExpressionOrString(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(b_inner)-[r_inner]->(c_inner)")
      .|.argument("b_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .allNodeScan("me")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "b", "c", "r").withRows(
      inPartialOrder(
        Seq(
          Seq(
            Array(n1, n1, emptyList(), emptyList(), emptyList())
          ),
          Seq(
            Array(n2, n2, emptyList(), emptyList(), emptyList()),
            Array(n2, n1, listOf(n2), listOf(n1), listOf(r21)),
            Array(n2, n3, listOf(n2), listOf(n3), listOf(r23))
          ),
          Seq(
            Array(n3, n3, emptyList(), emptyList(), emptyList())
          )
        )
      )
    )
  }

  // (n1:A) <- (n2) -> (n3)
  private def smallTreeGraph = {
    given {
      val n1 = tx.createNode(label("A"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      n1.setProperty("foo", 0)
      n2.setProperty("foo", 1)
      n3.setProperty("foo", 2)
      val r21 = n2.createRelationshipTo(n1, withName("R"))
      val r23 = n2.createRelationshipTo(n3, withName("R"))
      (n1, n2, n3, r21, r23)
    }
  }

  test("should handle unused anonymous end-node - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, emptyList(), emptyList(), emptyList()),
          Array(n0, listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23))
        ),
        Seq(
          Array(n1, emptyList(), emptyList(), emptyList()),
          Array(n1, listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
        )
      )
    ))
  }

  test("should respect lower limit - when lower limit is same as upper limit - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{2,2} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23))
        ),
        Seq(
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
        )
      )
    ))
  }

  test("should respect lower limit - when lower limit is lower than upper limit - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{1,2} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23))
        ),
        Seq(
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
        )
      )
    ))
  }

  test("should respect relationship uniqueness - with leveraged order on LHS") {
    // given
    //  (n0:START)(n1:START)
    //          ↓  ↓
    //          (n2)
    //        ↗     ↘
    //     (n5)     (n3)
    //        ↖     ↙
    //          (n4)
    val (n0, n1, n2, n3, n4, n5, r02, r12, r23, r34, r45, r52) = given {
      val n0 = tx.createNode(label("START"))
      val n1 = tx.createNode(label("START"))
      n0.setProperty("foo", 0)
      n1.setProperty("foo", 1)
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()

      val r02 = n0.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r45 = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r52 = n5.createRelationshipTo(n2, RelationshipType.withName("R"))
      (n0, n1, n2, n3, n4, n5, r02, r12, r23, r34, r45, r52)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,*} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23)),
          Array(n0, n4, listOf(n0, n2, n3), listOf(n2, n3, n4), listOf(r02, r23, r34)),
          Array(n0, n5, listOf(n0, n2, n3, n4), listOf(n2, n3, n4, n5), listOf(r02, r23, r34, r45)),
          Array(n0, n2, listOf(n0, n2, n3, n4, n5), listOf(n2, n3, n4, n5, n2), listOf(r02, r23, r34, r45, r52))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
          Array(n1, n4, listOf(n1, n2, n3), listOf(n2, n3, n4), listOf(r12, r23, r34)),
          Array(n1, n5, listOf(n1, n2, n3, n4), listOf(n2, n3, n4, n5), listOf(r12, r23, r34, r45)),
          Array(n1, n2, listOf(n1, n2, n3, n4, n5), listOf(n2, n3, n4, n5, n2), listOf(r12, r23, r34, r45, r52))
        )
      )
    ))
  }

  test(
    "should respect relationship uniqueness when repeated relationship occurs at different depths - short - with leveraged order on LHS"
  ) {
    // given
    //     (n0:START)(n1:START)
    //             ↓  ↓
    //             (n2)
    //             ↓  ↑
    //             (n3)
    //             ↓  ↑
    //             (n4)

    val (n0, n1, n2, n3, n4, r02, r12, r23, r32, r34, r43) = given {
      val n0 = tx.createNode(label("START"))
      val n1 = tx.createNode(label("START"))
      n0.setProperty("foo", 0)
      n1.setProperty("foo", 1)
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val r02 = n0.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r32 = n3.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r43 = n4.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n0, n1, n2, n3, n4, r02, r12, r23, r32, r34, r43)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,*} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    /*
    n1
    n1-n2
    n1-n2-n3
    n1-n2-n3-n2
    n1-n2-n3-n4
    n1-n2-n3-n2-n3          !filtered out!
    n1-n2-n3-n4-n3
    n1-n2-n3-n4-n3-n4       !filtered out!
    n1-n2-n3-n4-n3-n2
    n1-n2-n3-n4-n3-n2-n3    !filtered out!
     */

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23)),
          Array(n0, n2, listOf(n0, n2, n3), listOf(n2, n3, n2), listOf(r02, r23, r32)),
          Array(n0, n4, listOf(n0, n2, n3), listOf(n2, n3, n4), listOf(r02, r23, r34)),
          Array(n0, n3, listOf(n0, n2, n3, n4), listOf(n2, n3, n4, n3), listOf(r02, r23, r34, r43)),
          Array(n0, n2, listOf(n0, n2, n3, n4, n3), listOf(n2, n3, n4, n3, n2), listOf(r02, r23, r34, r43, r32))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
          Array(n1, n2, listOf(n1, n2, n3), listOf(n2, n3, n2), listOf(r12, r23, r32)),
          Array(n1, n4, listOf(n1, n2, n3), listOf(n2, n3, n4), listOf(r12, r23, r34)),
          Array(n1, n3, listOf(n1, n2, n3, n4), listOf(n2, n3, n4, n3), listOf(r12, r23, r34, r43)),
          Array(n1, n2, listOf(n1, n2, n3, n4, n3), listOf(n2, n3, n4, n3, n2), listOf(r12, r23, r34, r43, r32))
        )
      )
    ))
  }

  test("should respect relationship uniqueness of several relationship variables - with leveraged order on LHS") {
    // given
    // (n0:START) (n1:START)
    //          ↓  ↓
    //          (n2)
    //        ↗     ↘
    //      (n5)     (n3)
    //        ↖     ↙
    //          (n4)
    val (n0, n1, n2, n3, n4, n5, r02, r12, r23, r34, r45, r52) = given {
      val n0 = tx.createNode(label("START"))
      val n1 = tx.createNode(label("START"))
      n0.setProperty("foo", 0)
      n1.setProperty("foo", 1)
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()
      val r02 = n0.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r45 = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r52 = n5.createRelationshipTo(n2, RelationshipType.withName("R"))
      (n0, n1, n2, n3, n4, n5, r02, r12, r23, r34, r45, r52)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->()-[]->(b)]{0,*} (you)`).withLeveragedOrder()
      .|.filterExpressionOrString("not ranon = r_inner", isRepeatTrailUnique("ranon"))
      .|.expandAll("(secret)-[ranon]->(b_inner)")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(secret)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n3, listOf(n0), listOf(n3), listOf(r02)),
          Array(n0, n5, listOf(n0, n3), listOf(n3, n5), listOf(r02, r34))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n3, listOf(n1), listOf(n3), listOf(r12)),
          Array(n1, n5, listOf(n1, n3), listOf(n3, n5), listOf(r12, r34))
        )
      )
    ))
  }

  test("should handle branched graph - with leveraged order on LHS") {
    //                     (n3) → (n5)
    //                    ↗
    // (n0:START) ↘     /
    //             (n2)
    // (n1:START) ↗     \
    //                    ↘
    //                     (n4) → (n6)
    val (n0, n1, n2, n3, n4, n5, n6, r02, r12, r23, r24, r35, r46) = given {
      val n0 = tx.createNode(label("START"))
      val n1 = tx.createNode(label("START"))
      n0.setProperty("foo", 0)
      n1.setProperty("foo", 1)
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()
      val n6 = tx.createNode()
      val r02 = n0.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r24 = n2.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r35 = n3.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r46 = n4.createRelationshipTo(n6, RelationshipType.withName("R"))
      (n0, n1, n2, n3, n4, n5, n6, r02, r12, r23, r24, r35, r46)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,3} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23)),
          Array(n0, n4, listOf(n0, n2), listOf(n2, n4), listOf(r02, r24)),
          Array(n0, n5, listOf(n0, n2, n3), listOf(n2, n3, n5), listOf(r02, r23, r35)),
          Array(n0, n6, listOf(n0, n2, n4), listOf(n2, n4, n6), listOf(r02, r24, r46))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
          Array(n1, n4, listOf(n1, n2), listOf(n2, n4), listOf(r12, r24)),
          Array(n1, n5, listOf(n1, n2, n3), listOf(n2, n3, n5), listOf(r12, r23, r35)),
          Array(n1, n6, listOf(n1, n2, n4), listOf(n2, n4, n6), listOf(r12, r24, r46))
        )
      )
    ))
  }

  test("should be able to reference LHS from RHS - with leveraged order on LHS") {
    /*
      (n0) ↘
            (n2) → (n3) → (n4)
      (n1) ↗
     */
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = given {
      val n0 = tx.createNode()
      n0.setProperty("prop", 42)
      val n1 = tx.createNode()
      n1.setProperty("prop", 1)
      val n2 = tx.createNode()
      n2.setProperty("prop", 1)
      val n3 = tx.createNode()
      n3.setProperty("prop", 42)
      val n4 = tx.createNode()
      n4.setProperty("prop", 42)
      n0.setProperty("foo", 0)
      n1.setProperty("foo", 1)
      n2.setProperty("foo", 2)
      n3.setProperty("foo", 3)
      n4.setProperty("foo", 4)
      val r02 = n0.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      (n0, n1, n2, n3, n4, r02, r12, r23, r34)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,3} (you)`).withLeveragedOrder()
      .|.filter("b_inner.prop = me.prop")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .allNodeScan("me")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList())
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12))
        ),
        Seq(
          Array(n2, n2, emptyList(), emptyList(), emptyList())
        ),
        Seq(
          Array(n3, n3, emptyList(), emptyList(), emptyList()),
          Array(n3, n4, listOf(n3), listOf(n4), listOf(r34))
        ),
        Seq(
          Array(n4, n4, emptyList(), emptyList(), emptyList())
        )
      )
    ))
  }

  test("should work when columns are introduced on top of trail - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "r2")
      .projection("r AS r2")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "r2").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02), listOf(r02)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), listOf(r02, r23))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), listOf(r12)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(r12, r23))
        )
      )
    ))
  }

  test("should work when concatenated - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    // given: (me:START) [(a)-[r]->(b)]{0,1} (you) [(c)-[rr]->(d)]{0,1} (other)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "other", "a", "b", "r", "c", "d", "rr")
      .trail(`(you) [(c)-[rr]->(d)]{0,1} (other)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("me", "you", "c_inner")
      .trail(`(me) [(a)-[r]->(b)]{0,1} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // 0: (n1)
    // 1: (n1) → (n2)

    // 0: (n1)
    // 0: (n1) → (n2)
    // 1: (n1) → (n2)
    // 1: (n1) → (n2) → (n3)

    // then
    runtimeResult should beColumns("me", "other", "a", "b", "r", "c", "d", "rr").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n0, n2, emptyList(), emptyList(), emptyList(), listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02), emptyList(), emptyList(), emptyList()),
          Array(n0, n3, listOf(n0), listOf(n2), listOf(r02), listOf(n2), listOf(n3), listOf(r23))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
          Array(n1, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23))
        )
      )
    ))
  }

  test("should respect relationship uniqueness when concatenated - with leveraged order on LHS") {
    // given
    // (n0:START)  (n1:START)
    //          ↓  ↓
    //          (n2)
    //        ↗     ↘
    //      (n4) <- (n3)
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34, r42) = given {
      val n0 = tx.createNode(label("START"))
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      n0.setProperty("foo", 0)
      n1.setProperty("foo", 1)
      val r02 = n0.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r42 = n4.createRelationshipTo(n2, RelationshipType.withName("R"))
      (n0, n1, n2, n3, n4, r02, r12, r23, r34, r42)
    }

    // given: (me:START) [(a)-[r]->(b)]{0,2} (you) [(c)-[rr]->(d)]{0,2} (other)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "other", "a", "b", "r", "c", "d", "rr")
      .trail(`(you) [(c)-[rr]->(d)]{0,2} (other)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("me", "c_inner")
      .trail(`(me) [(a)-[r]->(b)]{0,3} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // 0: (n1)
    // 1: (n1) → (n2)
    // 2: (n1) → (n2) → (n3)
    // 3: (n1) → (n2) → (n3) → (n4)

    // 0: (n1)|
    // 0: (n1)| → (n2)
    // 0: (n1)| → (n2) → (n3)
    // 1: (n1) → (n2)|
    // 1: (n1) → (n2)| → (n3)
    // 1: (n1) → (n2)| → (n3) → (n4)
    // 2: (n1) → (n2) → (n3)|
    // 2: (n1) → (n2) → (n3)| → (n4)
    // 2: (n1) → (n2) → (n3)| → (n4) → (n2)
    // 3: (n1) → (n2) → (n3) → (n4)|
    // 3: (n1) → (n2) → (n3) → (n4)| → (n2)

    // then
    runtimeResult should beColumns("me", "other", "a", "b", "r", "c", "d", "rr").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n0, n2, emptyList(), emptyList(), emptyList(), listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n3, emptyList(), emptyList(), emptyList(), listOf(n0, n2), listOf(n2, n3), listOf(r02, r23)),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02), emptyList(), emptyList(), emptyList()),
          Array(n0, n3, listOf(n0), listOf(n2), listOf(r02), listOf(n2), listOf(n3), listOf(r23)),
          Array(n0, n4, listOf(n0), listOf(n2), listOf(r02), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), emptyList(), emptyList(), emptyList()),
          Array(n0, n4, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), listOf(n3), listOf(n4), listOf(r34)),
          Array(
            n0,
            n2,
            listOf(n0, n2),
            listOf(n2, n3),
            listOf(r02, r23),
            listOf(n3, n4),
            listOf(n4, n2),
            listOf(r34, r42)
          ),
          Array(
            n0,
            n4,
            listOf(n0, n2, n3),
            listOf(n2, n3, n4),
            listOf(r02, r23, r34),
            emptyList(),
            emptyList(),
            emptyList()
          ),
          Array(
            n0,
            n2,
            listOf(n0, n2, n3),
            listOf(n2, n3, n4),
            listOf(r02, r23, r34),
            listOf(n4),
            listOf(n2),
            listOf(r42)
          )
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
          Array(n1, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
          Array(n1, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
          Array(n1, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34)),
          Array(
            n1,
            n2,
            listOf(n1, n2),
            listOf(n2, n3),
            listOf(r12, r23),
            listOf(n3, n4),
            listOf(n4, n2),
            listOf(r34, r42)
          ),
          Array(
            n1,
            n4,
            listOf(n1, n2, n3),
            listOf(n2, n3, n4),
            listOf(r12, r23, r34),
            emptyList(),
            emptyList(),
            emptyList()
          ),
          Array(
            n1,
            n2,
            listOf(n1, n2, n3),
            listOf(n2, n3, n4),
            listOf(r12, r23, r34),
            listOf(n4),
            listOf(n2),
            listOf(r42)
          )
        )
      )
    ))
  }

  test("should respect relationship uniqueness of previous relationships - with leveraged order on LHS") {
    // given
    //          ↗ (n0:START) ↘
    //     (n3)              (n2)
    //      ↑   ↘ (n1:START) ↗  |
    //      |                   |
    //       -------------------
    val (n0, n1, n2, n3, r02, r12, r23, r30, r31) = given {
      val n0 = tx.createNode(label("START"))
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      n0.setProperty("foo", 0)
      n1.setProperty("foo", 1)
      val r02 = n0.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r30 = n3.createRelationshipTo(n0, RelationshipType.withName("R"))
      val r31 = n3.createRelationshipTo(n1, RelationshipType.withName("R"))
      (n0, n1, n2, n3, r02, r12, r23, r30, r31)
    }

    // given: MATCH (a:START)-[e]->(b) (()-[f]->(c))+
    // MATCH (a)-[e]->(b) (b)((anon_inner)-[f_inner]-(c_inner))+ (anon_end)

    val `(anon_start) [()-[f]->(c)]{1,*} (anon_end)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "anon_end",
      innerStart = "anon_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("c_inner", "c")),
      groupRelationships = Set(("f_inner", "f")),
      innerRelationships = Set("f_inner"),
      previouslyBoundRelationships = Set("e"),
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "e", "b", "f", "c")
      .trail(`(anon_start) [()-[f]->(c)]{1,*} (anon_end)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("f_inner"))
      .|.expandAll("(anon_inner)-[f_inner]->(c_inner)")
      .|.argument("anon_inner")
      .sort("foo ASC")
      .projection("a.foo AS foo")
      .filter("a:START")
      .allRelationshipsScan("(a)-[e]->(b)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("a", "e", "b", "f", "c").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, r02, n2, listOf(r23), listOf(n3)),
          Array(n0, r02, n2, listOf(r23, r31), listOf(n3, n1)),
          Array(n0, r02, n2, listOf(r23, r30), listOf(n3, n0)),
          Array(n0, r02, n2, listOf(r23, r31, r12), listOf(n3, n1, n2))
        ),
        Seq(
          Array(n1, r12, n2, listOf(r23), listOf(n3)),
          Array(n1, r12, n2, listOf(r23, r30), listOf(n3, n0)),
          Array(n1, r12, n2, listOf(r23, r31), listOf(n3, n1)),
          Array(n1, r12, n2, listOf(r23, r30, r02), listOf(n3, n0, n2))
        )
      )
    ))
  }

  test("should produce rows with nullable slots - with leveraged order on LHS") {
    // given: MATCH (me:START) OPTIONAL MATCH (me) [(a)-[r]->(b)]{0,*} (you:User) RETURN *

    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b")
      .apply()
      .|.optional("me")
      .|.filter("you:User")
      .|.trail(`(me) [(a)-[r]->(b)]{0,*} (you)`).withLeveragedOrder()
      .|.|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.|.argument("me", "a_inner")
      .|.argument("me")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("me", "you", "a", "b").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, null, null, null)
        ),
        Seq(
          Array(n1, null, null, null)
        )
      )
    ))
  }

  test("should handle double relationship path with filter - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val `() ((a)->[r]->(b)->[s]->(c))+ ()` : TrailParameters = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_start",
      end = "anon_end",
      innerStart = "a_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("b_inner", "b"), ("c_inner", "c"), ("a_inner", "a")),
      groupRelationships = Set(("r_inner", "r"), ("s_inner", "s")),
      innerRelationships = Set("r_inner", "s_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c", "r", "s")
      .trail(`() ((a)->[r]->(b)->[s]->(c))+ ()`).withLeveragedOrder()
      .|.filterExpressionOrString("not s_inner = r_inner", isRepeatTrailUnique("s_inner"))
      .|.expandAll("(b_inner)-[s_inner]->(c_inner)")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument()
      .sort("foo ASC")
      .projection("anon_start.foo AS foo")
      .nodeByLabelScan("anon_start", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("a", "b", "c", "r", "s").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(listOf(n0), listOf(n2), listOf(n3), listOf(r02), listOf(r23))
        ),
        Seq(
          Array(listOf(n1), listOf(n2), listOf(n3), listOf(r12), listOf(r23))
        )
      )
    ))
  }

  test("should work with limit on top - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .limit(1)
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(rowCount(1))
  }

  test("should work with limit on rhs 1 - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.unwind("[1] AS ignore") // pipelined specific: does not need a filtering morsel
      .|.nonFuseable() // pipelined specific: force break to test where RHS output receives normal Morsel but RHS leaf requires FilteringMorsel
      .|.limit(1)
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
        )
      )
    ))
  }

  test("should work with limit on rhs 2 - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.limit(Int.MaxValue) // pipelined specific: test when RHS output receives FilteringMorsel & RHS leaf requires FilteringMorsel in different pipeline
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.nonFuseable()
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
        )
      )
    ))
  }

  test("should work with filter on top - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .filter(s"id(you)<>${n2.getId}")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
        )
      )
    ))
  }

  test("should work with filter on rhs 1 - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.filter(s"id(b_inner)<>${n3.getId}")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12))
        )
      )
    ))
  }

  test("should work with filter on rhs 2 - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.unwind("[1] AS ignore") // pipelined specific: does not need a filtering morsel
      .|.nonFuseable() // pipelined specific: force break to test where RHS output receives normal Morsel but RHS leaf requires FilteringMorsel
      .|.filter(s"id(b_inner)<>${n3.getId}")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12))
        )
      )
    ))
  }

  test(
    "should respect relationship uniqueness when repeated relationship occurs at different depths - long - with leveraged order on LHS"
  ) {
    // given
    //      (n3)
    //      ↓ ↑
    //      (n2)
    //       ↑ ↑
    //                                ←
    //      (n0:START {foo:1}) → (n4) → (n5)
    //      (n1:START {foo:2}) →      →
    //
    //      ↓ ↓
    //      (n6)
    //      ↓ ↑
    //      (n7)

    val (n0, n1, n2, n3, n4, n5, n6, n7, r02, r12, r23, r32, r04, r14, r45a, r45b, r54, r06, r16, r67, r76) = given {
      val n0 = tx.createNode(label("START"))
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()
      val n6 = tx.createNode()
      val n7 = tx.createNode()

      val r02 = n0.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r32 = n3.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r04 = n0.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r14 = n1.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r45a = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r45b = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r54 = n5.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r06 = n0.createRelationshipTo(n6, RelationshipType.withName("R"))
      val r16 = n1.createRelationshipTo(n6, RelationshipType.withName("R"))
      val r67 = n6.createRelationshipTo(n7, RelationshipType.withName("R"))
      val r76 = n7.createRelationshipTo(n6, RelationshipType.withName("R"))
      (n0, n1, n2, n3, n4, n5, n6, n7, r02, r12, r23, r32, r04, r14, r45a, r45b, r54, r06, r16, r67, r76)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me) [(a)-[r]->(b)]{0,*} (you)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    /*
    n0

    n0-[r02]-n2
    n0-[r04]-n4
    n0-[r06]-n6

    n0-[r02]-n2-[r23]-n3
    n0-[r04]-n4-[r45a]-n5
    n0-[r04]-n4-[r45b]-n5
    n0-[r06]-n6-[r67]-n7

    n0-[r02]-n2-[r23]-n3-[r32]-n2
    n0-[r04]-n4-[r45a]-n5-[r54]-n4
    n0-[r04]-n4-[r45b]-n5-[r54]-n4
    n0-[r06]-n6-[r67]-n7-[r76]-n6

    n0-[r02]-n2-[r23]-n3-[r32]-n2-[r23]-n3              !filtered!
    n0-[r04]-n4-[r45a]-n5-[r54]-n4-[r45b]-n5
    n0-[r04]-n4-[r45b]-n5-[r54]-n4-[r45a]-n5
    n0-[r06]-n6-[r67]-n7-[r76]-n6-[r67]-n7              !filtered!

    n0-[r04]-n4-[r45a]-n5-[r54]-n4-[r45b]-n5-[r54]-n4   !filtered!
    n0-[r04]-n4-[r45b]-n5-[r54]-n4-[r45a]-n5-[r54]-n4   !filtered!

    n1

    n1-[r12]-n2
    n1-[r14]-n4
    n1-[r16]-n6

    n1-[r12]-n2-[r23]-n3
    n1-[r14]-n4-[r45a]-n5
    n1-[r14]-n4-[r45b]-n5
    n1-[r16]-n6-[r67]-n7

    n1-[r12]-n2-[r23]-n3-[r32]-n2
    n1-[r14]-n4-[r45a]-n5-[r54]-n4
    n1-[r14]-n4-[r45b]-n5-[r54]-n4
    n1-[r16]-n6-[r67]-n7-[r76]-n6

    n1-[r12]-n2-[r23]-n3-[r32]-n2-[r23]-n3              !filtered!
    n1-[r14]-n4-[r45a]-n5-[r54]-n4-[r45b]-n5
    n1-[r14]-n4-[r45b]-n5-[r54]-n4-[r45a]-n5
    n1-[r16]-n6-[r67]-n7-[r76]-n6-[r67]-n7              !filtered!

    n1-[r14]-n4-[r45a]-n5-[r54]-n4-[r45b]-n5-[r54]-n4   !filtered!
    n1-[r14]-n4-[r45b]-n5-[r54]-n4-[r45a]-n5-[r54]-n4   !filtered!
     */

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, emptyList(), emptyList(), emptyList()),
          Array(n0, n2, listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n4, listOf(n0), listOf(n4), listOf(r04)),
          Array(n0, n6, listOf(n0), listOf(n6), listOf(r06)),
          Array(n0, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23)),
          Array(n0, n5, listOf(n0, n4), listOf(n4, n5), listOf(r04, r45a)),
          Array(n0, n5, listOf(n0, n4), listOf(n4, n5), listOf(r04, r45b)),
          Array(n0, n7, listOf(n0, n6), listOf(n6, n7), listOf(r06, r67)),
          Array(n0, n2, listOf(n0, n2, n3), listOf(n2, n3, n2), listOf(r02, r23, r32)),
          Array(n0, n4, listOf(n0, n4, n5), listOf(n4, n5, n4), listOf(r04, r45a, r54)),
          Array(n0, n4, listOf(n0, n4, n5), listOf(n4, n5, n4), listOf(r04, r45b, r54)),
          Array(n0, n6, listOf(n0, n6, n7), listOf(n6, n7, n6), listOf(r06, r67, r76)),
          Array(n0, n5, listOf(n0, n4, n5, n4), listOf(n4, n5, n4, n5), listOf(r04, r45a, r54, r45b)),
          Array(n0, n5, listOf(n0, n4, n5, n4), listOf(n4, n5, n4, n5), listOf(r04, r45b, r54, r45a))
        ),
        Seq(
          Array(n1, n1, emptyList(), emptyList(), emptyList()),
          Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n4, listOf(n1), listOf(n4), listOf(r14)),
          Array(n1, n6, listOf(n1), listOf(n6), listOf(r16)),
          Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
          Array(n1, n5, listOf(n1, n4), listOf(n4, n5), listOf(r14, r45a)),
          Array(n1, n5, listOf(n1, n4), listOf(n4, n5), listOf(r14, r45b)),
          Array(n1, n7, listOf(n1, n6), listOf(n6, n7), listOf(r16, r67)),
          Array(n1, n2, listOf(n1, n2, n3), listOf(n2, n3, n2), listOf(r12, r23, r32)),
          Array(n1, n4, listOf(n1, n4, n5), listOf(n4, n5, n4), listOf(r14, r45a, r54)),
          Array(n1, n4, listOf(n1, n4, n5), listOf(n4, n5, n4), listOf(r14, r45b, r54)),
          Array(n1, n6, listOf(n1, n6, n7), listOf(n6, n7, n6), listOf(r16, r67, r76)),
          Array(n1, n5, listOf(n1, n4, n5, n4), listOf(n4, n5, n4, n5), listOf(r14, r45a, r54, r45b)),
          Array(n1, n5, listOf(n1, n4, n5, n4), listOf(n4, n5, n4, n5), listOf(r14, r45b, r54, r45a))
        )
      )
    ))
  }

  test("should work with union on RHS and (0,2) repetitions - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .trail(`(you) [(c)-[rr]->(d)]{0,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.union()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.union()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, n0, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n0, n0, n2, emptyList(), emptyList(), emptyList(), listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n0, n3, emptyList(), emptyList(), emptyList(), listOf(n0, n2), listOf(n2, n3), listOf(r02, r23)),
          Array(n0, n2, n2, listOf(n0), listOf(n2), listOf(r02), emptyList(), emptyList(), emptyList()),
          Array(n0, n2, n3, listOf(n0), listOf(n2), listOf(r02), listOf(n2), listOf(n3), listOf(r23)),
          Array(n0, n2, n4, listOf(n0), listOf(n2), listOf(r02), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n0, n3, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), emptyList(), emptyList(), emptyList()),
          Array(n0, n3, n4, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), listOf(n3), listOf(n4), listOf(r34))
        ),
        Seq(
          Array(n1, n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n1, n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
          Array(n1, n2, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
          Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
          Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n1, n3, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
          Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
        )
      )
    ))
  }

  test("should work with union on RHS and (1,2) repetitions - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .trail(`(you) [(c)-[rr]->(d)]{1,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.union()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .trail(`(me) [(a)-[r]->(b)]{1,2} (you)`).withLeveragedOrder()
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.union()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n2, n3, listOf(n0), listOf(n2), listOf(r02), listOf(n2), listOf(n3), listOf(r23)),
          Array(n0, n2, n4, listOf(n0), listOf(n2), listOf(r02), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n0, n3, n4, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), listOf(n3), listOf(n4), listOf(r34))
        ),
        Seq(
          Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
          Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
        )
      )
    ))
  }

  test("should work with cartesian product on RHS and (0,2) repetitions - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .trail(`(you) [(c)-[rr]->(d)]{0,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.cartesianProduct()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .optional("me")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.cartesianProduct()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, n0, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n0, n0, n2, emptyList(), emptyList(), emptyList(), listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n0, n3, emptyList(), emptyList(), emptyList(), listOf(n0, n2), listOf(n2, n3), listOf(r02, r23)),
          Array(n0, n2, n2, listOf(n0), listOf(n2), listOf(r02), emptyList(), emptyList(), emptyList()),
          Array(n0, n2, n3, listOf(n0), listOf(n2), listOf(r02), listOf(n2), listOf(n3), listOf(r23)),
          Array(n0, n2, n4, listOf(n0), listOf(n2), listOf(r02), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n0, n3, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), emptyList(), emptyList(), emptyList()),
          Array(n0, n3, n4, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), listOf(n3), listOf(n4), listOf(r34))
        ),
        Seq(
          Array(n1, n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n1, n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
          Array(n1, n2, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
          Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
          Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n1, n3, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
          Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
        )
      )
    ))
  }

  test("should work with cartesian product on RHS and (1,2) repetitions - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .trail(`(you) [(c)-[rr]->(d)]{1,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.cartesianProduct()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .trail(`(me) [(a)-[r]->(b)]{1,2} (you)`).withLeveragedOrder()
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.cartesianProduct()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n2, n3, listOf(n0), listOf(n2), listOf(r02), listOf(n2), listOf(n3), listOf(r23)),
          Array(n0, n2, n4, listOf(n0), listOf(n2), listOf(r02), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n0, n3, n4, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), listOf(n3), listOf(n4), listOf(r34))
        ),
        Seq(
          Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
          Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
        )
      )
    ))
  }

  test("should work with join on RHS and (0,2) repetitions - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .trail(`(you) [(c)-[rr]->(d)]{0,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.nodeHashJoin("d_inner")
      .|.|.allNodeScan("d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expand("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("you", "c_inner")
      .optional("me")
      .trail(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.expand("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n0, n0, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n0, n0, n2, emptyList(), emptyList(), emptyList(), listOf(n0), listOf(n2), listOf(r02)),
          Array(n0, n0, n3, emptyList(), emptyList(), emptyList(), listOf(n0, n2), listOf(n2, n3), listOf(r02, r23)),
          Array(n0, n2, n2, listOf(n0), listOf(n2), listOf(r02), emptyList(), emptyList(), emptyList()),
          Array(n0, n2, n3, listOf(n0), listOf(n2), listOf(r02), listOf(n2), listOf(n3), listOf(r23)),
          Array(n0, n2, n4, listOf(n0), listOf(n2), listOf(r02), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n0, n3, n3, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), emptyList(), emptyList(), emptyList()),
          Array(n0, n3, n4, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), listOf(n3), listOf(n4), listOf(r34))
        ),
        Seq(
          Array(n1, n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
          Array(n1, n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
          Array(n1, n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
          Array(n1, n2, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
          Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
          Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n1, n3, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
          Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
        )
      )
    ))
  }

  test("should work with join on RHS and (1,2) repetitions - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .trail(`(you) [(c)-[rr]->(d)]{1,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.nodeHashJoin("d_inner")
      .|.|.allNodeScan("d_inner")
      .|.filterExpression(isRepeatTrailUnique("rr_inner"))
      .|.expand("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("you", "c_inner")
      .optional("me")
      .trail(`(me) [(a)-[r]->(b)]{1,2} (you)`).withLeveragedOrder()
      .|.optional("me")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expand("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .sort("foo ASC")
      .projection("me.foo AS foo")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "other", "a", "b", "r", "c", "d", "rr").withRows(inPartialOrder(
      Seq(
        Seq(
          Array(n0, n2, n3, listOf(n0), listOf(n2), listOf(r02), listOf(n2), listOf(n3), listOf(r23)),
          Array(n0, n2, n4, listOf(n0), listOf(n2), listOf(r02), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n0, n3, n4, listOf(n0, n2), listOf(n2, n3), listOf(r02, r23), listOf(n3), listOf(n4), listOf(r34))
        ),
        Seq(
          Array(n1, n2, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
          Array(n1, n2, n4, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n4), listOf(r23, r34)),
          Array(n1, n3, n4, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n4), listOf(r34))
        )
      )
    ))
  }

  test("complex case: chained trails - with leveraged order on LHS of all trails") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndPartiallyOrderedExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.filter("d_inner:LOOP")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.filter("b_inner:MIDDLE")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.filter("anon_end_inner:MIDDLE")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inPartialOrder(expectedResult)
    )
  }

  test("complex case: chained trails - with leveraged order on LHS of first trails") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.filter("d_inner:LOOP")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.filter("b_inner:MIDDLE")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.filter("anon_end_inner:MIDDLE")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inAnyOrder(expectedResult)
    )
  }

  test("complex case: chained trails - with leveraged order on LHS of last trail") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndPartiallyOrderedExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.filter("d_inner:LOOP")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.filter("b_inner:MIDDLE")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.filter("anon_end_inner:MIDDLE")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inPartialOrder(expectedResult)
    )
  }

  test("complex case: chained trails, solved with joins - with leveraged order on LHS of all trails") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndPartiallyOrderedExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withLeveragedOrder()
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.filter("d_inner:LOOP")
      .|.|.nodeHashJoin("d_inner")
      .|.|.|.allNodeScan("d_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.filter("b_inner:MIDDLE")
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.nodeHashJoin("anon_end_inner")
      .|.|.filter("anon_end_inner:MIDDLE")
      .|.|.allNodeScan("anon_end_inner")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inPartialOrder(expectedResult)
    )
  }

  test("complex case: chained trails, solved with joins - with leveraged order on LHS of first trails") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.filter("d_inner:LOOP")
      .|.|.nodeHashJoin("d_inner")
      .|.|.|.allNodeScan("d_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.filter("b_inner:MIDDLE")
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.nodeHashJoin("anon_end_inner")
      .|.|.filter("anon_end_inner:MIDDLE")
      .|.|.allNodeScan("anon_end_inner")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inAnyOrder(expectedResult)
    )
  }

  test("complex case: chained trails, solved with joins - with leveraged order on LHS of last trail") {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndPartiallyOrderedExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.filter("d_inner:LOOP")
      .|.|.nodeHashJoin("d_inner")
      .|.|.|.allNodeScan("d_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.filter("b_inner:MIDDLE")
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.nodeHashJoin("anon_end_inner")
      .|.|.filter("anon_end_inner:MIDDLE")
      .|.|.allNodeScan("anon_end_inner")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inPartialOrder(expectedResult)
    )
  }

  test(
    "complex case: chained trails, solved with joins, plus excessive plan complexity - with leveraged order on LHS of all trails"
  ) {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndPartiallyOrderedExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .valueHashJoin("left=right")

      // Join RHS (identical to LHS)
      .|.projection("[start, middle, end, a, b, r1, c, d, r2] AS right")
      // insert distinct to "cancel out" the effect of unioning two identical inputs together
      .|.distinct(
        "start AS start",
        "middle AS middle",
        "end AS end",
        "a AS a",
        "b AS b",
        "r1 AS r1",
        "c AS c",
        "d AS d",
        "r2 AS r2"
      )
      .|.union()
      // (on RHS of Join) Union RHS (identical to Union LHS)
      .|.|.optional("end")
      .|.|.filter("end:LOOP")
      .|.|.apply()
      .|.|.|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.|.|.filter("r2_inner IS NOT NULL")
      .|.|.|.|.optional("middle")
      .|.|.|.|.filter("d_inner:LOOP")
      .|.|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.|.limit(Long.MaxValue)
      .|.|.|.|.|.allNodeScan("d_inner")
      .|.|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.|.argument("middle", "c_inner")
      .|.|.|.argument("middle")
      .|.|.filter("middle:MIDDLE:LOOP")
      .|.|.trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.|.|.filter("b_inner:MIDDLE")
      .|.|.|.nodeHashJoin("b_inner")
      .|.|.|.|.allNodeScan("b_inner")
      .|.|.|.limit(Long.MaxValue)
      .|.|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.|.optional("start")
      .|.|.|.argument("firstMiddle", "a_inner")
      .|.|.trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.|.allNodeScan("anon_end_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.|.argument("start", "anon_start_inner")
      .|.|.sort("foo ASC")
      .|.|.projection("start.foo AS foo")
      .|.|.nodeByLabelScan("start", "START", IndexOrderNone)
      // (on RHS of Join) Union LHS (identical to Union RHS)
      .|.filter("end:LOOP")
      .|.apply()
      .|.|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.|.filter("d_inner:LOOP")
      .|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.allNodeScan("d_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.argument("middle", "c_inner")
      .|.|.argument("middle")
      .|.filter("middle:MIDDLE:LOOP")
      .|.trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.|.filter("b_inner:MIDDLE")
      .|.|.nodeHashJoin("b_inner")
      .|.|.|.allNodeScan("b_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.argument("firstMiddle", "a_inner")
      .|.trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.allNodeScan("anon_end_inner")
      .|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.argument("start", "anon_start_inner")
      .|.sort("foo ASC")
      .|.projection("start.foo AS foo")
      .|.nodeByLabelScan("start", "START", IndexOrderNone)

      // Join LHS (identical to RHS)
      .projection("[start, middle, end, a, b, r1, c, d, r2] AS left")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.limit(Long.MaxValue)
      .|.|.filter("d_inner:LOOP")
      .|.|.nodeHashJoin("d_inner")
      .|.|.|.allNodeScan("d_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .limit(Long.MaxValue)
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.filter("b_inner:MIDDLE")
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.nodeHashJoin("anon_end_inner")
      .|.|.filter("anon_end_inner:MIDDLE")
      .|.|.allNodeScan("anon_end_inner")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inPartialOrder(expectedResult)
    )
  }

  test(
    "complex case: chained trails, solved with joins, plus excessive plan complexity - with leveraged order on LHS of first trails"
  ) {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .valueHashJoin("left=right")

      // Join RHS (identical to LHS)
      .|.projection("[start, middle, end, a, b, r1, c, d, r2] AS right")
      // insert distinct to "cancel out" the effect of unioning two identical inputs together
      .|.distinct(
        "start AS start",
        "middle AS middle",
        "end AS end",
        "a AS a",
        "b AS b",
        "r1 AS r1",
        "c AS c",
        "d AS d",
        "r2 AS r2"
      )
      .|.union()
      // (on RHS of Join) Union RHS (identical to Union LHS)
      .|.|.optional("end")
      .|.|.filter("end:LOOP")
      .|.|.apply()
      .|.|.|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.|.|.filter("r2_inner IS NOT NULL")
      .|.|.|.|.optional("middle")
      .|.|.|.|.filter("d_inner:LOOP")
      .|.|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.|.limit(Long.MaxValue)
      .|.|.|.|.|.allNodeScan("d_inner")
      .|.|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.|.argument("middle", "c_inner")
      .|.|.|.argument("middle")
      .|.|.filter("middle:MIDDLE:LOOP")
      .|.|.trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.|.|.filter("b_inner:MIDDLE")
      .|.|.|.nodeHashJoin("b_inner")
      .|.|.|.|.allNodeScan("b_inner")
      .|.|.|.limit(Long.MaxValue)
      .|.|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.|.optional("start")
      .|.|.|.argument("firstMiddle", "a_inner")
      .|.|.trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.|.allNodeScan("anon_end_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.|.argument("start", "anon_start_inner")
      .|.|.sort("foo ASC")
      .|.|.projection("start.foo AS foo")
      .|.|.nodeByLabelScan("start", "START", IndexOrderNone)
      // (on RHS of Join) Union LHS (identical to Union RHS)
      .|.filter("end:LOOP")
      .|.apply()
      .|.|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.|.filter("d_inner:LOOP")
      .|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.allNodeScan("d_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.argument("middle", "c_inner")
      .|.|.argument("middle")
      .|.filter("middle:MIDDLE:LOOP")
      .|.trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.|.filter("b_inner:MIDDLE")
      .|.|.nodeHashJoin("b_inner")
      .|.|.|.allNodeScan("b_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.argument("firstMiddle", "a_inner")
      .|.trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.allNodeScan("anon_end_inner")
      .|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.argument("start", "anon_start_inner")
      .|.sort("foo ASC")
      .|.projection("start.foo AS foo")
      .|.nodeByLabelScan("start", "START", IndexOrderNone)

      // Join LHS (identical to RHS)
      .projection("[start, middle, end, a, b, r1, c, d, r2] AS left")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`)
      .|.|.limit(Long.MaxValue)
      .|.|.filter("d_inner:LOOP")
      .|.|.nodeHashJoin("d_inner")
      .|.|.|.allNodeScan("d_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .limit(Long.MaxValue)
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`).withLeveragedOrder()
      .|.filter("b_inner:MIDDLE")
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`).withLeveragedOrder()
      .|.nodeHashJoin("anon_end_inner")
      .|.|.filter("anon_end_inner:MIDDLE")
      .|.|.allNodeScan("anon_end_inner")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inAnyOrder(expectedResult)
    )
  }

  test(
    "complex case: chained trails, solved with joins, plus excessive plan complexity - with leveraged order on LHS of last trail"
  ) {
    // given
    //  (n0:START)                                                  (n6:LOOP)
    //             ↘             →                                ↗     |
    //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
    //             ↗             ←                                ↖     ↓
    //  (n2:START)                                                  (n7:LOOP)
    val expectedResult = complexGraphAndPartiallyOrderedExpectedResult

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2")
      .valueHashJoin("left=right")

      // Join RHS (identical to LHS)
      .|.projection("[start, middle, end, a, b, r1, c, d, r2] AS right")
      // insert distinct to "cancel out" the effect of unioning two identical inputs together
      .|.distinct(
        "start AS start",
        "middle AS middle",
        "end AS end",
        "a AS a",
        "b AS b",
        "r1 AS r1",
        "c AS c",
        "d AS d",
        "r2 AS r2"
      )
      .|.union()
      // (on RHS of Join) Union RHS (identical to Union LHS)
      .|.|.optional("end")
      .|.|.filter("end:LOOP")
      .|.|.apply()
      .|.|.|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.|.|.filter("r2_inner IS NOT NULL")
      .|.|.|.|.optional("middle")
      .|.|.|.|.filter("d_inner:LOOP")
      .|.|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.|.limit(Long.MaxValue)
      .|.|.|.|.|.allNodeScan("d_inner")
      .|.|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.|.argument("middle", "c_inner")
      .|.|.|.argument("middle")
      .|.|.sort("foo ASC")
      .|.|.projection("start.foo AS foo")
      .|.|.filter("middle:MIDDLE:LOOP")
      .|.|.trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.|.|.filter("b_inner:MIDDLE")
      .|.|.|.nodeHashJoin("b_inner")
      .|.|.|.|.allNodeScan("b_inner")
      .|.|.|.limit(Long.MaxValue)
      .|.|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.|.optional("start")
      .|.|.|.argument("firstMiddle", "a_inner")
      .|.|.trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.|.allNodeScan("anon_end_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.|.argument("start", "anon_start_inner")
      .|.|.nodeByLabelScan("start", "START", IndexOrderNone)
      // (on RHS of Join) Union LHS (identical to Union RHS)
      .|.filter("end:LOOP")
      .|.apply()
      .|.|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.|.filter("d_inner:LOOP")
      .|.|.|.nodeHashJoin("d_inner")
      .|.|.|.|.allNodeScan("d_inner")
      .|.|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.|.argument("middle", "c_inner")
      .|.|.argument("middle")
      .|.sort("foo ASC")
      .|.projection("start.foo AS foo")
      .|.filter("middle:MIDDLE:LOOP")
      .|.trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.|.filter("b_inner:MIDDLE")
      .|.|.nodeHashJoin("b_inner")
      .|.|.|.allNodeScan("b_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.|.argument("firstMiddle", "a_inner")
      .|.trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.|.nodeHashJoin("anon_end_inner")
      .|.|.|.filter("anon_end_inner:MIDDLE")
      .|.|.|.allNodeScan("anon_end_inner")
      .|.|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.|.argument("start", "anon_start_inner")
      .|.nodeByLabelScan("start", "START", IndexOrderNone)

      // Join LHS (identical to RHS)
      .projection("[start, middle, end, a, b, r1, c, d, r2] AS left")
      .filter("end:LOOP")
      .apply()
      .|.trail(`(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`).withLeveragedOrder()
      .|.|.limit(Long.MaxValue)
      .|.|.filter("d_inner:LOOP")
      .|.|.nodeHashJoin("d_inner")
      .|.|.|.allNodeScan("d_inner")
      .|.|.filterExpression(isRepeatTrailUnique("r2_inner"))
      .|.|.expandAll("(c_inner)-[r2_inner]->(d_inner)")
      .|.|.argument("middle", "c_inner")
      .|.argument("middle")
      .limit(Long.MaxValue)
      .sort("foo ASC")
      .projection("start.foo AS foo")
      .filter("middle:MIDDLE:LOOP")
      .trail(`(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`)
      .|.filter("b_inner:MIDDLE")
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
      .|.filterExpression(isRepeatTrailUnique("r1_inner"))
      .|.expandAll("(a_inner)-[r1_inner]->(b_inner)")
      .|.argument("firstMiddle", "a_inner")
      .trail(`(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`)
      .|.nodeHashJoin("anon_end_inner")
      .|.|.filter("anon_end_inner:MIDDLE")
      .|.|.allNodeScan("anon_end_inner")
      .|.filterExpression(isRepeatTrailUnique("anon_r_inner"))
      .|.expandAll("(anon_start_inner)-[anon_r_inner]->(anon_end_inner)")
      .|.argument("start", "anon_start_inner")
      .nodeByLabelScan("start", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2").withRows(
      inPartialOrder(expectedResult)
    )
  }

  test(
    "should project original order of items in group variables when solved in reverse direction - with leveraged order on lhs"
  ) {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph
    n1.setProperty("foo", 1)
    n2.setProperty("foo", 2)
    n3.setProperty("foo", 3)
    n4.setProperty("foo", 4)

    // MATCH (me:START) ((a)-[r]->(b)){0,*} (you) when solved in reversed direction (right to left, expand b <- a),
    // items in group variables should be projected "left to right" as written in the query
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .filter("me:START")
      .trail(`(you) [(b)<-[r]-(a)]{0, *} (me)`).withLeveragedOrder()
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(b_inner)<-[r_inner]-(a_inner)")
      .|.argument("you", "b_inner")
      .sort("foo ASC")
      .projection("you.foo AS foo")
      .allNodeScan("you")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(inOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n4, listOf(n1, n2, n3), listOf(n2, n3, n4), listOf(r12, r23, r34))
      )
    ))

  }

  test("should respect relationship uniqueness between inner relationships - with leveraged order on lhs") {

    // (n1:START) → (n2)
    val (n1, n2, r12) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      (n1, n2, r12)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "c", "r", "rr")
      .trail(TrailTestBase.`(me) [(a)-[r]->(b)<-[rr]-(c)]{0,1} (you)`).withLeveragedOrder()
      .|.filterExpressionOrString("not rr_inner = r_inner", isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(b_inner)<-[rr_inner]-(c_inner)")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("me", "you", "a", "b", "c", "r", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList())
      )
    ))
  }

  test("should respect relationship uniqueness between more inner relationships - with leveraged order on lhs") {

    // (n1:START) → (n2) -> (n3)
    val (n1, n2, n3, r12, r23) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n1, n2, n3, r12, r23)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "c", "d", "r", "rr", "rrr")
      .trail(TrailTestBase.`(me) [(a)-[r]->(b)-[rr]->(c)<-[rrr]-(d)]{0,1} (you)`).withLeveragedOrder()
      .|.filterExpressionOrString(
        "not rrr_inner = rr_inner",
        "not rrr_inner = r_inner",
        isRepeatTrailUnique("rrr_inner")
      )
      .|.expandAll("(c_inner)<-[rrr_inner]-(d_inner)")
      .|.filterExpressionOrString("not rr_inner = r_inner", isRepeatTrailUnique("rr_inner"))
      .|.expandAll("(b_inner)-[rr_inner]->(c_inner)")
      .|.filterExpression(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("me", "you", "a", "b", "c", "d", "r", "rr", "rrr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList())
      )
    ))
  }

  // (n0:START) ↘
  //              (n2) → (n3) → (n4)
  // (n1:START) ↗
  protected def smallDoubleChainGraph
    : (Node, Node, Node, Node, Node, Relationship, Relationship, Relationship, Relationship) = {
    given {
      val chain = chainGraphs(1, "R", "R", "R").head
      val _n0 = tx.createNode(label("START"))
      val _n1 = chain.nodeAt(0)
      val _n2 = chain.nodeAt(1)
      val _r02 = _n0.createRelationshipTo(_n2, RelationshipType.withName("R"))
      _n0.setProperty("foo", 0)
      _n1.setProperty("foo", 1)
      (
        _n0,
        _n1,
        _n2,
        chain.nodeAt(2),
        chain.nodeAt(3),
        _r02,
        chain.relationshipAt(0),
        chain.relationshipAt(1),
        chain.relationshipAt(2)
      )
    }
  }

  def complexGraphAndPartiallyOrderedExpectedResult(): Seq[Seq[Array[Object]]] = {
    OrderedTrailTestBase.complexGraphAndPartiallyOrderedExpectedResult(givenComplexGraph())
  }
}

object OrderedTrailTestBase {

  protected def listOf(values: AnyRef*): util.List[AnyRef] = TrailTestBase.listOf(values: _*)

  /**
   * NOTE: Expected result obviously assumes that certain (equivalent) plans are used, which is the case for all tests calling this method.
   *
   * Those tests all use plans that solve the following the trail configurations:
   *  1. (start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)
   *  2. (firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)
   *  3. (middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)
   *
   * And return result columns: "start", "firstMiddle", "middle", "end", "a", "b", "r1", "c", "d", "r2"
   *
   * The input graph must be [[complexGraph]], i.e.:
   *
   * (n0:START)                                                  (n6:LOOP)
   *            ↘             →                                ↗     |
   * (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
   *            ↗             ←                                ↖     ↓
   * (n2:START)                                                  (n7:LOOP)
   *
   */
  def complexGraphAndPartiallyOrderedExpectedResult(complexGraph: ComplexGraph): Seq[Seq[Array[Object]]] = {
    val (n0, n1, n2, n3, n4, n5, n6, n7, r03, r13, r23, r34a, r34b, r43, r45, r56, r67, r75) =
      ComplexGraph.unapply(complexGraph).get

    Seq(
      Seq(
        Array(n0, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), emptyList(), emptyList(), emptyList()),
        Array(n0, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), listOf(n5), listOf(n6), listOf(r56)),
        Array(
          n0,
          n3,
          n5,
          n7,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34a, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n0,
          n3,
          n5,
          n5,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34a, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(n0, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), emptyList(), emptyList(), emptyList()),
        Array(n0, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), listOf(n5), listOf(n6), listOf(r56)),
        Array(
          n0,
          n3,
          n5,
          n7,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34b, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n0,
          n3,
          n5,
          n5,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34b, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(
          n0,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          emptyList(),
          emptyList(),
          emptyList()
        ),
        Array(
          n0,
          n3,
          n5,
          n6,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5),
          listOf(n6),
          listOf(r56)
        ),
        Array(
          n0,
          n3,
          n5,
          n7,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n0,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(
          n0,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          emptyList(),
          emptyList(),
          emptyList()
        ),
        Array(
          n0,
          n3,
          n5,
          n6,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5),
          listOf(n6),
          listOf(r56)
        ),
        Array(
          n0,
          n3,
          n5,
          n7,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n0,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        )
      ),
      Seq(
        Array(n1, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), listOf(n5), listOf(n6), listOf(r56)),
        Array(
          n1,
          n3,
          n5,
          n7,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34a, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n1,
          n3,
          n5,
          n5,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34a, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(n1, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), listOf(n5), listOf(n6), listOf(r56)),
        Array(
          n1,
          n3,
          n5,
          n7,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34b, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n1,
          n3,
          n5,
          n5,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34b, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(
          n1,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          emptyList(),
          emptyList(),
          emptyList()
        ),
        Array(
          n1,
          n3,
          n5,
          n6,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5),
          listOf(n6),
          listOf(r56)
        ),
        Array(
          n1,
          n3,
          n5,
          n7,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n1,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(
          n1,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          emptyList(),
          emptyList(),
          emptyList()
        ),
        Array(
          n1,
          n3,
          n5,
          n6,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5),
          listOf(n6),
          listOf(r56)
        ),
        Array(
          n1,
          n3,
          n5,
          n7,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n1,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        )
      ),
      Seq(
        Array(n2, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), emptyList(), emptyList(), emptyList()),
        Array(n2, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34a, r45), listOf(n5), listOf(n6), listOf(r56)),
        Array(
          n2,
          n3,
          n5,
          n7,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34a, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n2,
          n3,
          n5,
          n5,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34a, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(n2, n3, n5, n5, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), emptyList(), emptyList(), emptyList()),
        Array(n2, n3, n5, n6, listOf(n3, n4), listOf(n4, n5), listOf(r34b, r45), listOf(n5), listOf(n6), listOf(r56)),
        Array(
          n2,
          n3,
          n5,
          n7,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34b, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n2,
          n3,
          n5,
          n5,
          listOf(n3, n4),
          listOf(n4, n5),
          listOf(r34b, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(
          n2,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          emptyList(),
          emptyList(),
          emptyList()
        ),
        Array(
          n2,
          n3,
          n5,
          n6,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5),
          listOf(n6),
          listOf(r56)
        ),
        Array(
          n2,
          n3,
          n5,
          n7,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n2,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34a, r43, r34b, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        ),
        Array(
          n2,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          emptyList(),
          emptyList(),
          emptyList()
        ),
        Array(
          n2,
          n3,
          n5,
          n6,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5),
          listOf(n6),
          listOf(r56)
        ),
        Array(
          n2,
          n3,
          n5,
          n7,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5, n6),
          listOf(n6, n7),
          listOf(r56, r67)
        ),
        Array(
          n2,
          n3,
          n5,
          n5,
          listOf(n3, n4, n3, n4),
          listOf(n4, n3, n4, n5),
          listOf(r34b, r43, r34a, r45),
          listOf(n5, n6, n7),
          listOf(n6, n7, n5),
          listOf(r56, r67, r75)
        )
      )
    )
  }
}
