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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.BitSet
import scala.language.implicitConversions

class JoinSolverStepTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  implicit def converter(s: Symbol): String = s.toString()

  private val pattern1 =
    PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private val pattern2 =
    PatternRelationship(v"r2", (v"b", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("does not join based on empty table") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]
    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b", v"c")
      joinSolverStep(qg)(registry, register[NodeConnection](pattern1, pattern2), table, ctx) should be(empty)
    }
  }

  test("joins plans that solve a single pattern relationship") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]

    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )
      val plan2 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "b", "r2", "c")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan2.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"b", v"c"))
      )

      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b", v"c")

      table.put(register[NodeConnection](pattern1), sorted = false, plan1)
      table.put(register[NodeConnection](pattern2), sorted = false, plan2)

      joinSolverStep(qg)(registry, register[NodeConnection](pattern1, pattern2), table, ctx).toSet should equal(Set(
        NodeHashJoin(Set(v"b"), plan1, plan2),
        NodeHashJoin(Set(v"b"), plan2, plan1)
      ))
    }
  }

  test("produces only node hash joins with sort on RHS") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]

    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )
      val plan2 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "b", "r2", "c")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan2.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"b", v"c"))
      )
      // extra-symbol is used to make `plan1 != plan1Sort`
      val plan1Sort = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b", "extra-symbol")
      ctx.staticComponents.planningAttributes.solveds.copy(plan1.id, plan1Sort.id)
      // another extra-symbol to avoid that `(plan1.availableSymbols intersect plan2.availableSymbols).nonEmpty`
      val plan2Sort = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "b", "r2", "c", "extra-symbol2")
      ctx.staticComponents.planningAttributes.solveds.copy(plan2.id, plan2Sort.id)

      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b", v"c")

      table.put(register[NodeConnection](pattern1), sorted = false, plan1)
      table.put(register[NodeConnection](pattern1), sorted = true, plan1Sort)
      table.put(register[NodeConnection](pattern2), sorted = false, plan2)
      table.put(register[NodeConnection](pattern2), sorted = true, plan2Sort)

      joinSolverStep(qg)(registry, register[NodeConnection](pattern1, pattern2), table, ctx).toSet should equal(Set(
        NodeHashJoin(Set(v"b"), plan1, plan2),
        NodeHashJoin(Set(v"b"), plan1, plan2Sort),
        NodeHashJoin(Set(v"b"), plan2, plan1),
        NodeHashJoin(Set(v"b"), plan2, plan1Sort)
      ))
    }
  }

  test("can produce a join for a single pattern relationship") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]
    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )
      val plan2 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan2.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"b"))
      )

      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b")

      table.put(register[NodeConnection](pattern1), sorted = false, plan1)
      table.put(register[NodeConnection](pattern2), sorted = false, plan2)

      joinSolverStep(qg)(registry, register[NodeConnection](pattern1, pattern2), table, ctx).toSet should equal(Set(
        NodeHashJoin(Set(v"b"), plan1, plan2),
        NodeHashJoin(Set(v"b"), plan2, plan1)
      ))
    }
  }

  test("does not join plans that do not overlap") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]
    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )
      val plan2 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "c", "r2", "d")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan2.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"c", v"d"))
      )

      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b", v"c", v"d")

      table.put(register[NodeConnection](pattern1), sorted = false, plan1)
      table.put(register[NodeConnection](pattern2), sorted = false, plan2)

      joinSolverStep(qg)(registry, register[NodeConnection](pattern1, pattern2), table, ctx) should be(empty)
    }
  }

  test("does join plans where available nodes are subset of available symbols") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry = IdRegistry[NodeConnection]
    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val plan1 =
        fakeLogicalPlanFor(
          ctx.staticComponents.planningAttributes,
          "a",
          "r1",
          "b",
          "c"
        ) // those will become available symbols
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"b"))
      ) // those will become available nodes
      val plan2 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "b", "c")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan2.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"b"))
      )
      // overlapping symbols plan1& plan2 => (b,c)
      // overlapping nodes   plan1&plan2  => (b)

      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b", v"c")

      table.put(register[NodeConnection](pattern1), sorted = false, plan1)
      table.put(register[NodeConnection](pattern2), sorted = false, plan2)

      joinSolverStep(qg, IGNORE_EXPAND_SOLUTIONS_FOR_TEST = true)(
        registry,
        register[NodeConnection](pattern1, pattern2),
        table,
        ctx
      ).toSet should equal(Set(
        NodeHashJoin(Set(v"b"), plan1, plan2),
        NodeHashJoin(Set(v"b"), plan2, plan1)
      ))
    }
  }

  test("does not join plans that overlap on non-nodes") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]

    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b", "x")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )
      val plan2 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "c", "r2", "d", "x")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan2.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"c", v"d"))
      )

      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b", v"c", v"d")

      table.put(register[NodeConnection](pattern1), sorted = false, plan1)
      table.put(register[NodeConnection](pattern2), sorted = false, plan2)

      joinSolverStep(qg)(registry, register[NodeConnection](pattern1, pattern2), table, ctx) should be(empty)
    }
  }

  test("does not join plans that overlap on nodes that are arguments") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]
    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b", "x")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b", v"x").addArgumentIds(Seq(v"x")))
      )
      val plan2 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "c", "r2", "d", "x")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan2.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"c", v"d", v"x").addArgumentIds(Seq(v"x")))
      )

      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b", v"c", v"d").addArgumentIds(Seq(v"x"))

      table.put(register[NodeConnection](pattern1), sorted = false, plan1)
      table.put(register[NodeConnection](pattern2), sorted = false, plan2)

      joinSolverStep(qg)(registry, register[NodeConnection](pattern1, pattern2), table, ctx) should be(empty)
    }
  }

  test("does join plans that overlap on arguments if all of the goal is compacted") {
    val table = IDPTable.empty[LogicalPlan]
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]
    new givenConfig().withLogicalPlanningContext { (_, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b", "c") // symbols
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"b").addArgumentIds(Seq(v"b")))
      ) // nodes
      val plan2 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "b", "c")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan2.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"b").addArgumentIds(Seq(v"b")))
      )

      val qg = QueryGraph.empty.addPatternNodes(v"a", v"b", v"c").addArgumentIds(Seq(v"b"))

      val id1: Goal = register[NodeConnection](pattern1)
      val id2: Goal = register[NodeConnection](pattern2)

      // Compact goals
      val compactedId1 = Goal(BitSet(registry.compact(id1.bitSet)))
      val compactedId2 = Goal(BitSet(registry.compact(id2.bitSet)))
      table.removeAllTracesOf(id1)

      // Table is not completely compacted
      table.put(id2, sorted = false, plan2)
      table.put(compactedId1, sorted = false, plan1)
      table.put(compactedId2, sorted = false, plan2)

      // Goal is completely compacted - should result in expandStillPossible == false
      joinSolverStep(qg)(registry, Goal(compactedId1.bitSet ++ compactedId2.bitSet), table, ctx).toSet should equal(Set(
        NodeHashJoin(Set(v"b"), plan1, plan2),
        NodeHashJoin(Set(v"b"), plan2, plan1)
      ))
    }
  }

  def register[X](patRels: X*)(implicit registry: IdRegistry[X]): Goal = Goal(registry.registerAll(patRels))
}
