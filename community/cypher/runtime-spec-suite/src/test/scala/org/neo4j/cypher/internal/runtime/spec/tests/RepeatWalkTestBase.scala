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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.WalkParameters
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.GraphCreation.ComplexGraph
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(aa) ((e)<-[rrr]-(f)){1,}) (g)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(b) ((d)-[rr]->(aa:A) WHERE EXISTS {...} ){1,} (a)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me) ((b)-[r]->(c) WHERE EXISTS {...} ){1,} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me) [(a)-[r]->(b)]{0,*} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me) [(a)-[r]->(b)]{0,1} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me) [(a)-[r]->(b)]{0,2} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me) [(a)-[r]->(b)]{0,3} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me) [(a)-[r]->(b)]{1,2} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me) [(a)-[r]->(b)]{2,2} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me) [(a)-[r]->(b)]{3,5} (you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(you) [(b)<-[r]-(a)]{0, *} (me)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(you) [(c)-[rr]->(d)]{0,1} (other)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(you) [(c)-[rr]->(d)]{0,2} (other)`
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase.`(you) [(c)-[rr]->(d)]{1,2} (other)`
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.kernel.api.procedure.CallableUserFunction.BasicUserFunction
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues.pathReference

import java.util
import java.util.Collections.emptyList

abstract class RepeatWalkTestBase[CONTEXT <: RuntimeContext](
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{2,2} (you)`)
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{1,2} (you)`)
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

  test("should disrespect relationship uniqueness") {
    // given
    //          (n1)
    //        ↗     ↘
    //     (n4)     (n2)
    //        ↖     ↙
    //          (n3)
    val (n1, n2, n3, n4, r12, r23, r34, r41) = givenGraph {
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{3,5} (you)`)
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
        ),
        Array(
          n1,
          n2,
          listOf(n1, n2, n3, n4, n1),
          listOf(n2, n3, n4, n1, n2),
          listOf(r12, r23, r34, r41, r12),
          pathReference(
            Array(n1.getId, n2.getId, n3.getId, n4.getId, n1.getId, n2.getId),
            Array(r12.getId, r23.getId, r34.getId, r41.getId, r12.getId)
          )
        )
      )
    ))
  }

  test("should handle branched graph") {
    //      (n2) → (n4)
    //     ↗
    // (n1)
    //     ↘
    //      (n3) → (n5)
    val (n1, n2, n3, n4, n5, r12, r13, r24, r35) = givenGraph {
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
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
    val (n1, n2, n3, n4, r12, r23, r34) = givenGraph {
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filter("b_inner.prop = me.prop")
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

  test("should work when columns are introduced on top of walk") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "r2")
      .projection("r AS r2")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{0,1} (other)`)
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("me", "you", "c_inner")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,1} (you)`)
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

  test("should work nested under semi apply") {
    val (n1, n2, n3, n4, _, _, _) = smallChainGraph
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me")
      .semiApply()
      .|.repeatWalk(`(me) [(a)-[r]->(b)]{0,*} (you)`)
      .|.|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.|.argument("me", "a_inner")
      .|.argument("me")
      .allNodeScan("me")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("me").withRows(singleColumn(Seq(n1, n2, n3, n4)))
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
      .|.repeatWalk(`(me) [(a)-[r]->(b)]{0,*} (you)`)
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
    val (n1, n2, n3, r1, r2) = givenGraph {
      val n1 = tx.createNode()
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r1 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r2 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n1, n2, n3, r1, r2)
    }
    val `() ((a)->[r]->(b)->[s]->(c))+ ()`: WalkParameters = WalkParameters(
      min = 1,
      max = Unlimited,
      start = "anon_start",
      end = "anon_end",
      innerStart = "a_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("b_inner", "b"), ("c_inner", "c"), ("a_inner", "a")),
      groupRelationships = Set(("r_inner", "r"), ("s_inner", "s")),
      reverseGroupVariableProjections = false,
      innerRelationships = Set(("r_inner"), ("s_inner")),
      ExpandAll,
      accumulators = Set.empty
    )
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c", "r", "s")
      .repeatWalk(`() ((a)->[r]->(b)->[s]->(c))+ ()`)
      .|.filter("not s_inner = r_inner")
      .|.expandAll("(b_inner)-[s_inner]->(c_inner)")
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.unwind("[1] AS ignore") // pipelined specific: does not need a filtering morsel
      .|.nonFuseable() // pipelined specific: force break to test where RHS output receives normal Morsel but RHS leaf requires FilteringMorsel
      .|.limit(1)
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.limit(Int.MaxValue) // pipelined specific: test when RHS output receives FilteringMorsel & RHS leaf requires FilteringMorsel in different pipeline
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
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

  test("should work with into") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val `(me) [(a)-[r]->(b)]{0,*} (you) ExpandInto` = `(me) [(a)-[r]->(b)]{0,*} (you)`
      .copy(expansionMode = ExpandInto)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "a", "b", "r")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("me"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,*} (you) ExpandInto`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .cartesianProduct()
      .|.nodeByIdSeek("you", Set.empty, n4.getId)
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "a", "b", "r").withRows(inAnyOrder(
      Seq(
        Array(n1, listOf(n1, n2, n3), listOf(n2, n3, n4), listOf(r12, r23, r34))
      )
    ))
  }

  test("should work with into when end node is projected from user function") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, _, r12, r23, _) = smallChainGraph

    val userFunction = new BasicUserFunction(
      UserFunctionSignature.functionSignature(new QualifiedName("user.custom.getSingleNode"))
        .out(Neo4jTypes.NTNode).threadSafe().build()
    ) {

      override def apply(ctx: Context, input: Array[AnyValue]): AnyValue = {
        ValueUtils.of(n3)
      }
    }

    registerFunction(userFunction)
    // Refresh the transaction so its ProcedureView snapshot includes the function we just registered.
    restartTx()

    val `(me) [(a)-[r]->(b)]{0,*} (you) ExpandInto` = `(me) [(a)-[r]->(b)]{0,*} (you)`
      .copy(expansionMode = ExpandInto)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "a", "b", "r", "you")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("me"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,*} (you) ExpandInto`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .projection(Map("you" -> function("user.custom.getSingleNode")))
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "a", "b", "r", "you").withRows(inAnyOrder(
      Seq(
        Array(n1, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), n3)
      )
    ))
  }

  test("should work with into when null is projected from user function") {
    // (n1:START) → (n2) → (n3) → (n4)
    smallChainGraph

    val userFunction = new BasicUserFunction(
      UserFunctionSignature.functionSignature(new QualifiedName("user.custom.getSingleNode"))
        .out(Neo4jTypes.NTNode).threadSafe().build()
    ) {

      override def apply(ctx: Context, input: Array[AnyValue]): AnyValue = {
        ValueUtils.of(null)
      }
    }

    registerFunction(userFunction)
    // Refresh the transaction so its ProcedureView snapshot includes the function we just registered.
    restartTx()

    val `(me) [(a)-[r]->(b)]{0,*} (you) ExpandInto` = `(me) [(a)-[r]->(b)]{0,*} (you)`
      .copy(expansionMode = ExpandInto)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "a", "b", "r", "you")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("me"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,*} (you) ExpandInto`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .projection(Map("you" -> function("user.custom.getSingleNode")))
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "a", "b", "r", "you").withNoRows()
  }

  test("should work with filter on rhs 1") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filter(s"id(b_inner)<>${n3.getId}")
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.unwind("[1] AS ignore") // pipelined specific: does not need a filtering morsel
      .|.nonFuseable() // pipelined specific: force break to test where RHS output receives normal Morsel but RHS leaf requires FilteringMorsel
      .|.filter(s"id(b_inner)<>${n3.getId}")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{0,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.union()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{1,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.union()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{1,2} (you)`)
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{0,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.cartesianProduct()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{1,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.cartesianProduct()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{1,2} (you)`)
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{0,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.nodeHashJoin("d_inner")
      .|.|.allNodeScan("d_inner")
      .|.expand("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
      .|.optional("me")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{1,2} (other)`)
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.nodeHashJoin("d_inner")
      .|.|.allNodeScan("d_inner")
      .|.expand("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("you", "c_inner")
      .optional("me")
      .projection(Map("path1" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{1,2} (you)`)
      .|.optional("me")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.limit(1)
      .|.nodeHashJoin("b_inner")
      .|.|.allNodeScan("b_inner")
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

  test("should project original order of items in group variables when solved in reverse direction") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    // MATCH (me:START) ((a)-[r]->(b)){0,*} (you) when solved in reversed direction (right to left, expand b <- a),
    // items in group variables should be projected "left to right" as written in the query
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .filter("me:START")
      .repeatWalk(`(you) [(b)<-[r]-(a)]{0, *} (me)`)
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
    val (n1, n2, r12) = givenGraph {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      (n1, n2, r12)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "c", "r", "rr")
      .repeatWalk(RepeatWalkTestBase.`(me) [(a)-[r]->(b)<-[rr]-(c)]{0,1} (you)`)
      .|.filter("not rr_inner = r_inner")
      .|.expandAll("(b_inner)<-[rr_inner]-(c_inner)")
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
    val (n1, n2, n3, r12, r23) = givenGraph {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n1, n2, n3, r12, r23)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "c", "d", "r", "rr", "rrr")
      .repeatWalk(RepeatWalkTestBase.`(me) [(a)-[r]->(b)-[rr]->(c)<-[rrr]-(d)]{0,1} (you)`)
      .|.filter(
        "not rrr_inner = r_inner",
        "not rrr_inner = rr_inner"
      )
      .|.expandAll("(c_inner)<-[rrr_inner]-(d_inner)")
      .|.filter("not rr_inner = r_inner")
      .|.expandAll("(b_inner)-[rr_inner]->(c_inner)")
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

  test("should work with nested walks on rhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = givenGraph {
      val n1 = tx.createNode(label("A"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r21 = n2.createRelationshipTo(n1, withName("R"))
      val r23 = n2.createRelationshipTo(n3, withName("R"))
      (n1, n2, n3, r21, r23)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "b", "c", "r")
      .repeatWalk(`(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`)
      .|.apply()
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.repeatWalk(`(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`)
      .|.|.|.filter("aa_inner:A")
      .|.|.|.expandAll("(bb_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("bb_inner", "b_inner")
      .|.|.argument("b_inner")
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

  test("should work with multiple nested walks on rhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = givenGraph {
      val n1 = tx.createNode(label("A"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r21 = n2.createRelationshipTo(n1, withName("R"))
      val r23 = n2.createRelationshipTo(n3, withName("R"))
      (n1, n2, n3, r21, r23)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "b", "c", "r")
      .repeatWalk(`(me) ((b)-[r]->(c) WHERE EXISTS {...} ){1,} (you)`)
      .|.apply()
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.repeatWalk(`(b) ((d)-[rr]->(aa:A) WHERE EXISTS {...} ){1,} (a)`)
      .|.|.|.filter("aa_inner:A")
      .|.|.|.apply()
      .|.|.|.|.limit(1)
      .|.|.|.|.repeatWalk(`(aa) ((e)<-[rrr]-(f)){1,}) (g)`)
      .|.|.|.|.|.expandAll("(e_inner)<-[rrr_inner]-(f_inner)")
      .|.|.|.|.|.argument("aa_inner", "e_inner")
      .|.|.|.|.argument("aa_inner")
      .|.|.|.expandAll("(d_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("b_inner", "d_inner")
      .|.|.argument("b_inner")
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`)
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

  test("should work with allReduce accumulator") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val `(me) [(a)-[r]->(b)]{0,2} (you) allReduce(acc=[],acc+b,NOT n3 IN acc)` =
      RepeatWalkTestBase.createMeYouWalkParameters(
        min = 0,
        max = Limited(2),
        accumulators = Set(("[]", "currAcc", "nextAcc"))
      )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you) allReduce(acc=[],acc+b,NOT n3 IN acc)`)
      .|.filter(s"NOT ${n3.getId} IN nextAcc")
      .|.projection("currAcc + [id(b_inner)] AS nextAcc")
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
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId)))
      )
    ))
  }

  test("should work with allReduce variable accumulator") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val `(me) [(a)-[r]->(b)]{0,2} (you) allReduce(acc=initialAcc,acc+b,[n2]=acc)` =
      RepeatWalkTestBase.createMeYouWalkParameters(
        min = 0,
        max = Limited(2),
        accumulators = Set(("initialAcc", "currAcc", "nextAcc"))
      )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you) allReduce(acc=initialAcc,acc+b,[n2]=acc)`)
      .|.filter(s"[${n2.getId}]=nextAcc")
      .|.projection("currAcc + [id(b_inner)] AS nextAcc")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .projection(s"[] AS initialAcc")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId)))
      )
    ))
  }

  test("should work with allReduce node property accumulator") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = givenGraph {
      val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph
      n1.setProperty("prop", 1)
      n2.setProperty("prop", 2)
      n3.setProperty("prop", 3) // row passing through n3 will be filtered out
      n4.setProperty("prop", 4)
      (n1, n2, n3, n4, r12, r23, r34)
    }

    val `(me) [(a)-[r]->(b)]{0,2} (you) allReduce(acc=initialAcc,acc+b.prop,[2]=acc)` =
      RepeatWalkTestBase.createMeYouWalkParameters(
        min = 0,
        max = Limited(2),
        accumulators = Set(("initialAcc", "currAcc", "nextAcc"))
      )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you) allReduce(acc=initialAcc,acc+b.prop,[2]=acc)`)
      .|.filter("[2]=nextAcc") // filter out row passing through n3
      .|.projection("currAcc + [b_inner.prop] AS nextAcc")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .projection(s"[] AS initialAcc")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId)))
      )
    ))
  }

  test("should work with allReduce accumulator where accumulator is accessed before allReduce projection") {
    // NOTE: accumulator should never be accessed early in the RHS like this
    //       the test is just intended to test slot allocation

    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val `(me) [(a)-[r]->(b)]{0,2} (you) allReduce(acc=[],acc+b,NOT n3 IN acc)` =
      RepeatWalkTestBase.createMeYouWalkParameters(
        min = 0,
        max = Limited(2),
        accumulators = Set(("[]", "currAcc", "nextAcc"))
      )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "path")
      .projection(Map("path" -> qppPath(varFor("me"), Seq(varFor("a"), varFor("r")), varFor("you"))))
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you) allReduce(acc=[],acc+b,NOT n3 IN acc)`)
      .|.filter(s"NOT ${n3.getId} IN nextAcc")
      .|.projection("currAcc + id(b_inner) AS nextAcc")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.nonFuseable() // noop, just intended to force pipeline break
      .|.filter("currAcc IS NOT NULL")
      .|.argument("me", "a_inner", "currAcc")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "path").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), pathReference(Array(n1.getId), Array.empty[Long])),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), pathReference(Array(n1.getId, n2.getId), Array(r12.getId)))
      )
    ))
  }

  protected def listOf(values: AnyRef*): util.List[AnyRef] = RepeatWalkTestBase.listOf(values: _*)

  //  (n0:START)                                                  (n6:LOOP)
  //             ↘             →                                ↗     |
  //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
  //             ↗             ←                                ↖     ↓
  //  (n2:START)                                                  (n7:LOOP)
  protected def givenComplexGraph(): ComplexGraph = {
    givenGraph(complexGraph())
  }

  // (n1) → (n2) → (n3) → (n4)
  protected def smallChainGraph: (Node, Node, Node, Node, Relationship, Relationship, Relationship) = {
    givenGraph {
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
    givenGraph {
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

object RepeatWalkTestBase {
  def listOf(values: AnyRef*): util.List[AnyRef] = java.util.List.of[AnyRef](values: _*)

  def createMeYouWalkParameters(
    min: Int,
    max: UpperBound,
    accumulators: Set[(String, String, String)] = Set.empty
  ): WalkParameters = {
    WalkParameters(
      min,
      max,
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      reverseGroupVariableProjections = false,
      innerRelationships = Set("r_inner"),
      ExpandAll,
      accumulators = accumulators
    )
  }

  private def createYouOtherWalkParameters(min: Int, max: UpperBound): WalkParameters = {
    WalkParameters(
      min,
      max,
      start = "you",
      end = "other",
      innerStart = "c_inner",
      innerEnd = "d_inner",
      groupNodes = Set(("c_inner", "c"), ("d_inner", "d")),
      groupRelationships = Set(("rr_inner", "rr")),
      reverseGroupVariableProjections = false,
      innerRelationships = Set(("rr_inner")),
      ExpandAll,
      accumulators = Set.empty
    )
  }

  val `(me) [(a)-[r]->(b)]{0,1} (you)`: WalkParameters =
    createMeYouWalkParameters(min = 0, max = Limited(1))

  val `(me) [(a)-[r]->(b)]{0,2} (you)`: WalkParameters =
    createMeYouWalkParameters(min = 0, max = Limited(2))

  val `(me) [(a)-[r]->(b)]{0,3} (you)`: WalkParameters =
    createMeYouWalkParameters(min = 0, max = Limited(3))

  val `(me) [(a)-[r]->(b)]{0,*} (you)`: WalkParameters =
    createMeYouWalkParameters(min = 0, max = Unlimited)

  val `(me) [(a)-[r]->(b)]{1,1} (you)`: WalkParameters =
    createMeYouWalkParameters(min = 1, max = Limited(1))

  val `(me) [(a)-[r]->(b)]{1,2} (you)`: WalkParameters =
    createMeYouWalkParameters(min = 1, max = Limited(2))

  val `(me) [(a)-[r]->(b)]{2,2} (you)`: WalkParameters =
    createMeYouWalkParameters(min = 2, max = Limited(2))

  val `(me) [(a)-[r]->(b)]{3,5} (you)`: WalkParameters =
    createMeYouWalkParameters(min = 3, max = Limited(5))

  val `(you) [(c)-[rr]->(d)]{0,1} (other)`: WalkParameters =
    createYouOtherWalkParameters(min = 0, max = Limited(1))

  val `(you) [(c)-[rr]->(d)]{0,2} (other)`: WalkParameters =
    createYouOtherWalkParameters(min = 0, max = Limited(2))

  val `(you) [(c)-[rr]->(d)]{1,2} (other)`: WalkParameters =
    createYouOtherWalkParameters(min = 1, max = Limited(2))

  val `(me) [(a)-[r]->()-[]->(b)]{0,*} (you)`: WalkParameters = WalkParameters(
    min = 0,
    max = Unlimited,
    start = "me",
    end = "you",
    innerStart = "a_inner",
    innerEnd = "b_inner",
    groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
    groupRelationships = Set(("r_inner", "r")),
    reverseGroupVariableProjections = false,
    innerRelationships = Set("r_inner"),
    ExpandAll,
    accumulators = Set.empty
  )

  val `(start:START) [()-[]->(:MIDDLE)]{1, 1} (firstMiddle:MIDDLE)`: WalkParameters = WalkParameters(
    min = 1,
    max = Limited(1),
    start = "start",
    end = "firstMiddle",
    innerStart = "anon_start_inner",
    innerEnd = "anon_end_inner",
    groupNodes = Set(),
    groupRelationships = Set(),
    reverseGroupVariableProjections = false,
    innerRelationships = Set("r_inner"),
    ExpandAll,
    accumulators = Set.empty
  )

  val `(firstMiddle) [(a)-[r1]->(b:MIDDLE)]{0, *} (middle:MIDDLE:LOOP)`: WalkParameters = WalkParameters(
    min = 0,
    max = Unlimited,
    start = "firstMiddle",
    end = "middle",
    innerStart = "a_inner",
    innerEnd = "b_inner",
    groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
    groupRelationships = Set(("r1_inner", "r1")),
    reverseGroupVariableProjections = false,
    innerRelationships = Set("r1_inner"),
    ExpandAll,
    accumulators = Set.empty
  )

  val `(middle) [(c)-[r2]->(d:LOOP)]{0, *} (end:LOOP)`: WalkParameters = WalkParameters(
    min = 0,
    max = Unlimited,
    start = "middle",
    end = "end",
    innerStart = "c_inner",
    innerEnd = "d_inner",
    groupNodes = Set(("c_inner", "c"), ("d_inner", "d")),
    groupRelationships = Set(("r2_inner", "r2")),
    reverseGroupVariableProjections = false,
    innerRelationships = Set("r2_inner"),
    ExpandAll,
    accumulators = Set.empty
  )

  val `(you) [(b)<-[r]-(a)]{0, *} (me)`: WalkParameters =
    WalkParameters(
      min = 0,
      max = Unlimited,
      start = "you",
      end = "me",
      innerStart = "b_inner",
      innerEnd = "a_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      reverseGroupVariableProjections = true,
      innerRelationships = Set("r_inner"),
      ExpandAll,
      accumulators = Set.empty
    )

  val `(me) [(a)-[r]->(b)<-[rr]-(c)]{0,1} (you)`: WalkParameters =
    WalkParameters(
      min = 0,
      max = UpperBound.Limited(1),
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c")),
      groupRelationships = Set(("r_inner", "r"), ("rr_inner", "rr")),
      reverseGroupVariableProjections = false,
      innerRelationships = Set("r_inner", "rr_inner"),
      ExpandAll,
      accumulators = Set.empty
    )

  val `(me) ((b)-[r]->(c) WHERE EXISTS {...} ){1,} (you)`: WalkParameters = WalkParameters(
    1,
    UpperBound.Unlimited,
    "me",
    "you",
    "b_inner",
    "c_inner",
    Set(("b_inner", "b"), ("c_inner", "c")),
    Set(("r_inner", "r")),
    false,
    innerRelationships = Set("r_inner"),
    ExpandAll,
    accumulators = Set.empty
  )

  val `(b) ((d)-[rr]->(aa:A) WHERE EXISTS {...} ){1,} (a)`: WalkParameters = WalkParameters(
    1,
    UpperBound.Unlimited,
    "b_inner",
    "a",
    "d_inner",
    "aa_inner",
    Set(("d_inner", "d"), ("aa_inner", "aa")),
    Set(("rr_inner", "rr")),
    false,
    innerRelationships = Set("rr_inner"),
    ExpandAll,
    accumulators = Set.empty
  )

  val `(aa) ((e)<-[rrr]-(f)){1,}) (g)`: WalkParameters = WalkParameters(
    1,
    UpperBound.Unlimited,
    "aa_inner",
    "g",
    "e_inner",
    "f_inner",
    Set(("e_inner", "e"), ("f_inner", "f")),
    Set(("rrr_inner", "rrr")),
    false,
    innerRelationships = Set("rrr_inner"),
    ExpandAll,
    accumulators = Set.empty
  )

  val `(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`: WalkParameters =
    WalkParameters(
      min = 0,
      max = UpperBound.Unlimited,
      start = "me",
      end = "you",
      innerStart = "b_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("b_inner", "b"), ("c_inner", "c")),
      groupRelationships = Set(("r_inner", "r")),
      reverseGroupVariableProjections = false,
      innerRelationships = Set("r_inner"),
      ExpandAll,
      accumulators = Set.empty
    )

  val `(me) [(a)-[r]->(b)-[rr]->(c)<-[rrr]-(d)]{0,1} (you)`: WalkParameters =
    WalkParameters(
      min = 0,
      max = UpperBound.Limited(1),
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "d_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
      groupRelationships = Set(("r_inner", "r"), ("rr_inner", "rr"), ("rrr_inner", "rrr")),
      reverseGroupVariableProjections = false,
      innerRelationships = Set("r_inner", "rr_inner", "rrr_inner"),
      ExpandAll,
      accumulators = Set.empty
    )

  val `(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`: WalkParameters = WalkParameters(
    min = 0,
    max = UpperBound.Unlimited,
    start = "b_inner",
    end = "a",
    innerStart = "bb_inner",
    innerEnd = "aa_inner",
    groupNodes = Set(("bb_inner", "bb"), ("aa_inner", "aa")),
    groupRelationships = Set(("rr_inner", "rr")),
    reverseGroupVariableProjections = false,
    innerRelationships = Set("rr_inner"),
    ExpandAll,
    accumulators = Set.empty
  )
}

trait OrderedWalkTestBase[CONTEXT <: RuntimeContext] {
  self: RepeatWalkTestBase[CONTEXT] =>

  test("should work with multiple nested walks - with leveraged order on lhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = smallTreeGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "b", "c", "r")
      .repeatWalk(`(me) ((b)-[r]->(c) WHERE EXISTS {...} ){1,} (you)`).withLeveragedOrder()
      .|.apply()
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.repeatWalk(`(b) ((d)-[rr]->(aa:A) WHERE EXISTS {...} ){1,} (a)`).withLeveragedOrder()
      .|.|.|.filter("aa_inner:A")
      .|.|.|.apply()
      .|.|.|.|.limit(1)
      .|.|.|.|.repeatWalk(`(aa) ((e)<-[rrr]-(f)){1,}) (g)`).withLeveragedOrder()
      .|.|.|.|.|.expandAll("(e_inner)<-[rrr_inner]-(f_inner)")
      .|.|.|.|.|.argument("aa_inner", "e_inner")
      .|.|.|.|.argument("aa_inner")
      .|.|.|.expandAll("(d_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("b_inner", "d_inner")
      .|.|.argument("b_inner")
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

  test("should work with nested walks on rhs - with leveraged order on lhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = smallTreeGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "b", "c", "r")
      .repeatWalk(
        `(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`
      ).withLeveragedOrder()
      .|.apply()
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.repeatWalk(`(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`)
      .|.|.|.filter("aa_inner:A")
      .|.|.|.expandAll("(bb_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("bb_inner", "b_inner")
      .|.|.argument("b_inner")
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
    givenGraph {
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{2,2} (you)`).withLeveragedOrder()
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{1,2} (you)`).withLeveragedOrder()
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

  test("should handle branched graph - with leveraged order on LHS") {
    //                     (n3) → (n5)
    //                    ↗
    // (n0:START) ↘     /
    //             (n2)
    // (n1:START) ↗     \
    //                    ↘
    //                     (n4) → (n6)
    val (n0, n1, n2, n3, n4, n5, n6, r02, r12, r23, r24, r35, r46) = givenGraph {
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,3} (you)`).withLeveragedOrder()
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
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = givenGraph {
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,3} (you)`).withLeveragedOrder()
      .|.filter("b_inner.prop = me.prop")
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

  test("should work when columns are introduced on top of walk - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "r2")
      .projection("r AS r2")
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{0,1} (other)`).withLeveragedOrder()
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("me", "you", "c_inner")
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,1} (you)`).withLeveragedOrder()
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

  test("should work nested under semi apply - with leveraged order on LHS") {
    val (n1, n2, n3, n4, _, _, _) = smallChainGraph
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me")
      .semiApply()
      .|.repeatWalk(`(me) [(a)-[r]->(b)]{0,*} (you)`).withLeveragedOrder()
      .|.|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.|.argument("me", "a_inner")
      .|.argument("me")
      .sort("id ASC")
      .projection("id(me) as id")
      .allNodeScan("me")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = Seq(n1, n2, n3, n4).map(Array(_))
    runtimeResult should beColumns("me").withRows(inOrder(expected))
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
      .|.repeatWalk(`(me) [(a)-[r]->(b)]{0,*} (you)`).withLeveragedOrder()
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

    val `() ((a)->[r]->(b)->[s]->(c))+ ()`: WalkParameters = WalkParameters(
      min = 1,
      max = Unlimited,
      start = "anon_start",
      end = "anon_end",
      innerStart = "a_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("b_inner", "b"), ("c_inner", "c"), ("a_inner", "a")),
      groupRelationships = Set(("r_inner", "r"), ("s_inner", "s")),
      reverseGroupVariableProjections = false,
      innerRelationships = Set("r_inner", "s_inner"),
      ExpandAll,
      accumulators = Set.empty
    )
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c", "r", "s")
      .repeatWalk(`() ((a)->[r]->(b)->[s]->(c))+ ()`).withLeveragedOrder()
      .|.expandAll("(b_inner)-[s_inner]->(c_inner)")
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.unwind("[1] AS ignore") // pipelined specific: does not need a filtering morsel
      .|.nonFuseable() // pipelined specific: force break to test where RHS output receives normal Morsel but RHS leaf requires FilteringMorsel
      .|.limit(1)
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.limit(Int.MaxValue) // pipelined specific: test when RHS output receives FilteringMorsel & RHS leaf requires FilteringMorsel in different pipeline
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.filter(s"id(b_inner)<>${n3.getId}")
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
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.unwind("[1] AS ignore") // pipelined specific: does not need a filtering morsel
      .|.nonFuseable() // pipelined specific: force break to test where RHS output receives normal Morsel but RHS leaf requires FilteringMorsel
      .|.filter(s"id(b_inner)<>${n3.getId}")
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

  test("should work with union on RHS and (0,2) repetitions - with leveraged order on LHS") {
    // (n0:START) ↘
    //              (n2) → (n3) → (n4)
    // (n1:START) ↗
    val (n0, n1, n2, n3, n4, r02, r12, r23, r34) = smallDoubleChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "other", "a", "b", "r", "c", "d", "rr")
      .optional("me")
      .repeatWalk(`(you) [(c)-[rr]->(d)]{0,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.union()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{1,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.union()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .repeatWalk(`(me) [(a)-[r]->(b)]{1,2} (you)`).withLeveragedOrder()
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{0,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.cartesianProduct()
      .|.|.argument("me", "a_inner")
      .|.argument("me", "a_inner")
      .optional("me")
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{1,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.cartesianProduct()
      .|.|.argument("you", "c_inner")
      .|.argument("you", "c_inner")
      .optional("me")
      .repeatWalk(`(me) [(a)-[r]->(b)]{1,2} (you)`).withLeveragedOrder()
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{0,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.nodeHashJoin("d_inner")
      .|.|.allNodeScan("d_inner")
      .|.expand("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("you", "c_inner")
      .optional("me")
      .repeatWalk(`(me) [(a)-[r]->(b)]{0,2} (you)`).withLeveragedOrder()
      .|.optional("me")
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
      .repeatWalk(`(you) [(c)-[rr]->(d)]{1,2} (other)`).withLeveragedOrder()
      .|.sort("d_inner ASC")
      .|.distinct("d_inner  AS d_inner")
      .|.nodeHashJoin("d_inner")
      .|.|.allNodeScan("d_inner")
      .|.expand("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("you", "c_inner")
      .optional("me")
      .repeatWalk(`(me) [(a)-[r]->(b)]{1,2} (you)`).withLeveragedOrder()
      .|.optional("me")
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
      .repeatWalk(`(you) [(b)<-[r]-(a)]{0, *} (me)`).withLeveragedOrder()
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

  test("should disrespect relationship uniqueness between inner relationships - with leveraged order on lhs") {

    // (n1:START) → (n2)
    val (n1, n2, r12) = givenGraph {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      (n1, n2, r12)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "c", "r", "rr")
      .repeatWalk(RepeatWalkTestBase.`(me) [(a)-[r]->(b)<-[rr]-(c)]{0,1} (you)`).withLeveragedOrder()
      .|.expandAll("(b_inner)<-[rr_inner]-(c_inner)")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("me", "you", "a", "b", "c", "r", "rr").withRows(inAnyOrder(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n1, listOf(n1), listOf(n2), listOf(n1), listOf(r12), listOf(r12))
      )
    ))
  }

  test("should respect relationship uniqueness between more inner relationships - with leveraged order on lhs") {

    // (n1:START) → (n2) -> (n3)
    val (n1, n2, n3, r12, r23) = givenGraph {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n1, n2, n3, r12, r23)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "c", "d", "r", "rr", "rrr")
      .repeatWalk(RepeatWalkTestBase.`(me) [(a)-[r]->(b)-[rr]->(c)<-[rrr]-(d)]{0,1} (you)`).withLeveragedOrder()
      .|.filter(
        "not rrr_inner = rr_inner",
        "not rrr_inner = r_inner"
      )
      .|.expandAll("(c_inner)<-[rrr_inner]-(d_inner)")
      .|.expandAll("(b_inner)-[rr_inner]->(c_inner)")
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
    givenGraph {
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

}

object OrderedWalkTestBase {
  protected def listOf(values: AnyRef*): util.List[AnyRef] = RepeatWalkTestBase.listOf(values: _*)
}
