/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, NodeHashJoin}

class JoinSolverStepTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  implicit def converter(s: Symbol): String = s.toString()

  val pattern1 = PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val pattern2 = PatternRelationship("r2", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  val table = new IDPTable[LogicalPlan]()

  test("does not join based on empty table") {
    implicit val registry = IdRegistry[PatternRelationship]
    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val qg = QueryGraph.empty.addPatternNodes("a", "b", "c")
      joinSolverStep(qg)(registry, register(pattern1, pattern2), table, ctx, solveds) should be(empty)
    }
  }

  test("joins plans that solve a single pattern relationship") {
    implicit val registry = IdRegistry[PatternRelationship]

    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor(solveds, cardinalities, "a", "r1", "b")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b")))
      val plan2 = fakeLogicalPlanFor(solveds, cardinalities, "b", "r2", "c")
      solveds.set(plan2.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("b", "c")))

      val qg = QueryGraph.empty.addPatternNodes("a", "b", "c")

      table.put(register(pattern1), plan1)
      table.put(register(pattern2), plan2)

      joinSolverStep(qg)(registry, register(pattern1, pattern2), table, ctx, solveds).toSet should equal(Set(
        NodeHashJoin(Set("b"), plan1, plan2),
        NodeHashJoin(Set("b"), plan2, plan1)
      ))
    }
  }

  test("can produce a join for a single pattern relationship") {
    implicit val registry = IdRegistry[PatternRelationship]
    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor("a", "r1", "b")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b")))
      val plan2 = fakeLogicalPlanFor("b")
      solveds.set(plan2.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("b")))

      val qg = QueryGraph.empty.addPatternNodes("a", "b")

      table.put(register(pattern1), plan1)
      table.put(register(pattern2), plan2)

      joinSolverStep(qg)(registry, register(pattern1, pattern2), table, ctx, solveds).toSet should equal(Set(
        NodeHashJoin(Set("b"), plan1, plan2),
        NodeHashJoin(Set("b"), plan2, plan1)
      ))
    }
  }

  test("does not join plans that do not overlap") {
    implicit val registry = IdRegistry[PatternRelationship]
    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor("a", "r1", "b")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b")))
      val plan2 = fakeLogicalPlanFor("c", "r2", "d")
      solveds.set(plan2.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("c", "d")))

      val qg = QueryGraph.empty.addPatternNodes("a", "b", "c", "d")

      table.put(register(pattern1), plan1)
      table.put(register(pattern2), plan2)

      joinSolverStep(qg)(registry, register(pattern1, pattern2), table, ctx, solveds) should be(empty)
    }
  }

  test("does not join plans that overlap on non-nodes") {
    implicit val registry = IdRegistry[PatternRelationship]

    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor("a", "r1", "b", "x")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b")))
      val plan2 = fakeLogicalPlanFor("c", "r2", "d", "x")
      solveds.set(plan2.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("c", "d")))

      val qg = QueryGraph.empty.addPatternNodes("a", "b", "c", "d")

      table.put(register(pattern1), plan1)
      table.put(register(pattern2), plan2)

      joinSolverStep(qg)(registry, register(pattern1, pattern2), table, ctx, solveds) should be(empty)
    }
  }

  test("does not join plans that overlap on nodes that are arguments") {
    implicit val registry = IdRegistry[PatternRelationship]
    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor("a", "r1", "b", "x")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b", 'x).addArgumentIds(Seq('x))))
      val plan2 = fakeLogicalPlanFor("c", "r2", "d", "x")
      solveds.set(plan2.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("c", "d", 'x).addArgumentIds(Seq('x))))

      val qg = QueryGraph.empty.addPatternNodes("a", "b", "c", "d").addArgumentIds(Seq('x))

      table.put(register(pattern1), plan1)
      table.put(register(pattern2), plan2)

      joinSolverStep(qg)(registry, register(pattern1, pattern2), table, ctx, solveds) should be(empty)
    }
  }

  def register[X](patRels: X*)(implicit registry: IdRegistry[X]) = registry.registerAll(patRels)
}
