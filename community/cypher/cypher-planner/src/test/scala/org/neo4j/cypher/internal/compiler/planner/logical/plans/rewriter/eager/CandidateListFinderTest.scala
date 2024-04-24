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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.compiler.helpers.AnnotatedLogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.CandidateListFinder.CandidateList
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ConflictFinder.ConflictingPlanPair
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerWhereNeededRewriter.ChildrenIds
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

//noinspection ZeroIndexToHead
class CandidateListFinderTest extends CypherFunSuite {

  test("Simple linear plan, 1 conflict") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .fakeLeafPlan("n").annotate("p2")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p2")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2"))), conflicts(0))
    )
  }

  test("Should filter a candidate list already containing an EagerLogicalPlan") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .sort()
      .fakeLeafPlan("n").annotate("p2")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p2")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should be(empty)
  }

  test("Simple linear plan, 2 conflicts") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .expand("(m)-->(o)").annotate("p2")
      .expand("(n)-->(m)").annotate("p3")
      .fakeLeafPlan("n").annotate("p4")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p3")), Set.empty),
      ConflictingPlanPair(Ref(p.get("p2")), Ref(p.get("p4")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2")), Ref(p.get("p3"))), conflicts(0)),
      CandidateList(List(Ref(p.get("p3")), Ref(p.get("p4"))), conflicts(1))
    )
  }

  test("Does not accept if SemiApply has writes on its RHS") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .semiApply()
      .|.create(createNode("p", "P"))
      .|.argument("n")
      .fakeLeafPlan("n").annotate("p2")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p2")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    the[IllegalStateException] thrownBy CandidateListFinder.findCandidateLists(p.plan, conflicts) should have message (
      "Eagerness analysis does not support if the RHS of a SemiApply contains writes."
    )
  }

  test("Apply LHS vs RHS: Must be eagerized on LHS") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.expand("(m)-->(o)").annotate("p1")
      .|.argument("m")
      .expand("(n)-->(m)").annotate("p2")
      .fakeLeafPlan("n").annotate("p3")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p3")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2")), Ref(p.get("p3"))), conflicts(0))
    )
  }

  test("Apply LHS vs Top: Must be eagerized on LHS or Top") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .apply().annotate("p2")
      .|.expand("(m)-->(o)")
      .|.argument("m")
      .expand("(n)-->(m)").annotate("p3")
      .fakeLeafPlan("n").annotate("p4")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p4")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2")), Ref(p.get("p3")), Ref(p.get("p4"))), conflicts(0))
    )
  }

  test("Apply RHS vs Top: Must be eagerized on Top") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .apply().annotate("p2")
      .|.expand("(m)-->(o)")
      .|.argument("m").annotate("p3")
      .expand("(n)-->(m)")
      .fakeLeafPlan("n")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p3")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2"))), conflicts(0))
    )
  }

  test("CartesianProduct LHS vs RHS: Must be eagerized on LHS") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n")
      .cartesianProduct()
      .|.expand("(m)-->(o)").annotate("p1")
      .|.argument("m")
      .expand("(n)-->(m)").annotate("p2")
      .fakeLeafPlan("n").annotate("p3")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p3")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2")), Ref(p.get("p3"))), conflicts(0))
    )
  }

  test("CartesianProduct LHS vs Top: Must be eagerized on LHS or Top") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .cartesianProduct().annotate("p2")
      .|.expand("(m)-->(o)")
      .|.argument("m")
      .expand("(n)-->(m)").annotate("p3")
      .fakeLeafPlan("n").annotate("p4")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p4")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2")), Ref(p.get("p3")), Ref(p.get("p4"))), conflicts(0))
    )
  }

  test("CartesianProduct RHS vs Top: Must be eagerized on Top") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .cartesianProduct().annotate("p2")
      .|.expand("(m)-->(o)")
      .|.argument("m").annotate("p3")
      .expand("(n)-->(m)")
      .fakeLeafPlan("n")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p3")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2"))), conflicts(0))
    )
  }

  test("CartesianProduct on the RHS of Apply") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("a").annotate("PR")
      .apply().annotate("Apply")
      .|.cartesianProduct().annotate("CP")
      .|.|.expand("(c)-->(d)").annotate("d")
      .|.|.fakeLeafPlan("c").annotate("c")
      .|.fakeLeafPlan("b").annotate("b")
      .fakeLeafPlan("a").annotate("a")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("a")), Ref(p.get("b")), Set.empty),
      ConflictingPlanPair(Ref(p.get("a")), Ref(p.get("c")), Set.empty),
      ConflictingPlanPair(Ref(p.get("a")), Ref(p.get("CP")), Set.empty),
      ConflictingPlanPair(Ref(p.get("b")), Ref(p.get("c")), Set.empty),
      ConflictingPlanPair(Ref(p.get("b")), Ref(p.get("PR")), Set.empty),
      ConflictingPlanPair(Ref(p.get("c")), Ref(p.get("PR")), Set.empty),
      ConflictingPlanPair(Ref(p.get("c")), Ref(p.get("d")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("a"))), conflicts(0)),
      CandidateList(List(Ref(p.get("a"))), conflicts(1)),
      CandidateList(List(Ref(p.get("a"))), conflicts(2)),
      CandidateList(List(Ref(p.get("b"))), conflicts(3)),
      CandidateList(List(Ref(p.get("Apply"))), conflicts(4)),
      CandidateList(List(Ref(p.get("Apply"))), conflicts(5)),
      CandidateList(List(Ref(p.get("c"))), conflicts(6))
    )
  }

  test("AssertSameNode LHS vs RHS: Not supported") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n")
      .assertSameNode("n")
      .|.expand("(m)-->(o)").annotate("p1")
      .|.argument("m")
      .expand("(n)-->(m)").annotate("p2")
      .fakeLeafPlan("n").annotate("p3")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p3")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    the[IllegalStateException] thrownBy CandidateListFinder.findCandidateLists(p.plan, conflicts) should have message (
      "We do not expect conflicts between the two branches of a AssertSameNode yet."
    )
  }

  test("AssertSameNode on the RHS of Apply") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("a").annotate("PR")
      .apply().annotate("Apply")
      .|.assertSameNode("a").annotate("ASN")
      .|.|.expand("(c)-->(d)").annotate("d")
      .|.|.fakeLeafPlan("c").annotate("c")
      .|.fakeLeafPlan("b").annotate("b")
      .fakeLeafPlan("a").annotate("a")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("a")), Ref(p.get("b")), Set.empty),
      ConflictingPlanPair(Ref(p.get("a")), Ref(p.get("c")), Set.empty),
      ConflictingPlanPair(Ref(p.get("a")), Ref(p.get("ASN")), Set.empty),
      ConflictingPlanPair(Ref(p.get("b")), Ref(p.get("PR")), Set.empty),
      ConflictingPlanPair(Ref(p.get("c")), Ref(p.get("PR")), Set.empty),
      ConflictingPlanPair(Ref(p.get("c")), Ref(p.get("d")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("a"))), conflicts(0)),
      CandidateList(List(Ref(p.get("a"))), conflicts(1)),
      CandidateList(List(Ref(p.get("a"))), conflicts(2)),
      CandidateList(List(Ref(p.get("Apply"))), conflicts(3)),
      CandidateList(List(Ref(p.get("Apply"))), conflicts(4)),
      CandidateList(List(Ref(p.get("c"))), conflicts(5))
    )
  }

  test("Join LHS vs RHS: Must not be eagerized") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n")
      .nodeHashJoin("n")
      .|.expand("(m)-->(o)").annotate("p1")
      .|.argument("m")
      .expand("(n)-->(m)").annotate("p2")
      .fakeLeafPlan("n").annotate("p3")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p3")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should be(empty)
  }

  test("Join LHS vs Top: Must not be eagerized") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .nodeHashJoin("n").annotate("p2")
      .|.expand("(m)-->(o)")
      .|.argument("m")
      .expand("(n)-->(m)").annotate("p3")
      .fakeLeafPlan("n").annotate("p4")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p4")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should be(empty)
  }

  test("Join RHS vs Top: Must be eagerized on RHS or Top") {
    val p = new AnnotatedLogicalPlanBuilder()
      .produceResults("n").annotate("p1")
      .nodeHashJoin("n").annotate("p2")
      .|.expand("(m)-->(o)").annotate("p3")
      .|.argument("m").annotate("p4")
      .expand("(n)-->(m)")
      .fakeLeafPlan("n")
      .build()

    val conflicts = Seq(
      ConflictingPlanPair(Ref(p.get("p1")), Ref(p.get("p4")), Set.empty)
    )

    implicit val childrenIds: ChildrenIds = childrenIdsForPlan(p.plan)
    val result = CandidateListFinder.findCandidateLists(p.plan, conflicts)
    result should contain theSameElementsAs Seq(
      CandidateList(List(Ref(p.get("p2")), Ref(p.get("p3")), Ref(p.get("p4"))), conflicts(0))
    )
  }

  private def childrenIdsForPlan(lp: LogicalPlan): ChildrenIds = {
    val childrenIds = new ChildrenIds
    LogicalPlans.simpleFoldPlan(())(lp, (_, p) => childrenIds.recordChildren(p))
    childrenIds
  }
}
