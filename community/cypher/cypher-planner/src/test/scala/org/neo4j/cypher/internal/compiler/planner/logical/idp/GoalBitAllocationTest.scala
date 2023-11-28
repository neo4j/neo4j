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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.BitSet
import scala.collection.immutable.ListSet

class GoalBitAllocationTest extends CypherFunSuite with AstConstructionTestSupport {

  test("calculates correct sub-goals for full goal") {
    val gba = GoalBitAllocation(3, 5, (0 until 4).map(_ => BitSet.empty))
    val fullGoal = Goal(BitSet(1 to 8: _*))
    gba.componentsGoal(fullGoal) should equal(Goal(BitSet(1, 2, 3)))
    gba.optionalMatchesGoal(fullGoal) should equal(Goal(BitSet(4, 5, 6, 7, 8)))
  }

  test("calculates correct sub-goals for sub goal") {
    val gba = GoalBitAllocation(3, 5, (0 until 4).map(_ => BitSet.empty))
    val subGoal = Goal(BitSet(1, 2, 5, 7))
    gba.componentsGoal(subGoal) should equal(Goal(BitSet(1, 2)))
    gba.optionalMatchesGoal(subGoal) should equal(Goal(BitSet(5, 7)))
  }

  test("goal with no optional matches is solvable") {
    val gba = GoalBitAllocation(3, 5, (0 until 4).map(_ => BitSet.empty))
    gba.goalIsSolvable(registry(8), Goal(BitSet(1, 2))) should be(true)
  }

  test("goal with no optional matches is solvable, with compaction") {
    val gba = GoalBitAllocation(3, 5, (0 until 4).map(_ => BitSet.empty))
    val r = registry(8)
    val c = r.compact(BitSet(1, 2))
    gba.goalIsSolvable(r, Goal(BitSet(c))) should be(true)
  }

  test("goal with no dependant optional matches is solvable") {
    val gba = GoalBitAllocation(
      3,
      5,
      Seq(
        BitSet(),
        BitSet(1),
        BitSet(),
        BitSet(2, 3),
        BitSet(2)
      )
    )
    gba.goalIsSolvable(registry(8), Goal(BitSet(4, 6))) should be(true)
  }

  test("goal with no dependant optional matches is solvable, with compaction") {
    val gba = GoalBitAllocation(
      3,
      5,
      Seq(
        BitSet(),
        BitSet(1),
        BitSet(),
        BitSet(2, 3),
        BitSet(2)
      )
    )
    val r = registry(8)
    val c = r.compact(BitSet(4, 6))
    gba.goalIsSolvable(registry(8), Goal(BitSet(c))) should be(true)
  }

  test("goal with dependant optional matches and dependencies missing is not solvable") {
    val gba = GoalBitAllocation(
      3,
      5,
      Seq(
        BitSet(),
        BitSet(1),
        BitSet(),
        BitSet(2, 3),
        BitSet(4)
      )
    )
    val r = registry(8)
    gba.goalIsSolvable(r, Goal(BitSet(5))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(2, 3, 5, 6))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(1, 2, 7))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(1, 2, 3, 8))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(1, 2, 4, 7, 8))) should be(false)
  }

  test("goal with dependant optional matches and dependencies missing is not solvable, with compaction") {
    val gba = GoalBitAllocation(
      3,
      5,
      Seq(
        BitSet(),
        BitSet(1),
        BitSet(),
        BitSet(2, 3),
        BitSet(4)
      )
    )
    val r = registry(8)
    val c12 = r.compact(BitSet(1, 2))
    val c34 = r.compact(BitSet(3, 4))

    gba.goalIsSolvable(r, Goal(BitSet(5))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(c34, 5))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(c12, 7))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(c34, 7))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(c12, 8))) should be(false)

    val c3468 = r.compact(BitSet(c34, 6, 8))

    gba.goalIsSolvable(r, Goal(BitSet(5))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(c3468, 5))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(c12, 7))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(c3468, 7))) should be(false)
    gba.goalIsSolvable(r, Goal(BitSet(c12, 8))) should be(false)
  }

  test("goal with dependant optional matches and dependencies available is solvable") {
    val gba = GoalBitAllocation(
      3,
      5,
      Seq(
        BitSet(),
        BitSet(1),
        BitSet(),
        BitSet(2, 3),
        BitSet(4)
      )
    )
    val r = registry(8)
    gba.goalIsSolvable(r, Goal(BitSet(1, 5))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(1, 2, 3, 5, 6))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(1, 2, 3, 7))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(4, 8))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(1, 2, 4, 8))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(2, 3, 4, 7, 8))) should be(true)
  }

  test("goal with dependant optional matches and dependencies available is solvable, with compaction") {
    val gba = GoalBitAllocation(
      3,
      5,
      Seq(
        BitSet(),
        BitSet(1),
        BitSet(),
        BitSet(2, 3),
        BitSet(4)
      )
    )
    val r = registry(8)
    val c12 = r.compact(BitSet(1, 2))
    val c34 = r.compact(BitSet(3, 4))

    gba.goalIsSolvable(r, Goal(BitSet(c12, 5))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(c12, 5, 6))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(c12, c34, 7))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(c34, 8))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(c12, c34, 8))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(c12, c34, 7, 8))) should be(true)

    val c3468 = r.compact(BitSet(c34, 6, 8))

    gba.goalIsSolvable(r, Goal(BitSet(c12, 5))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(c12, 5, 6))) should be(true)
    gba.goalIsSolvable(r, Goal(BitSet(c12, c3468, 7))) should be(true)
  }

  test("GoalBitAllocation.create captures correct dependencies") {
    // GIVEN
    // 0: Sorted
    val components = ListSet(
      QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq(relTypeName("R")), SimplePatternLength))
      ), // 1
      QueryGraph(patternNodes = Set("c", "d")), // 2
      QueryGraph(
        patternNodes = Set("e", "f", "g"),
        shortestRelationshipPatterns = Set(ShortestRelationshipPattern(
          Some(v"p"),
          PatternRelationship(v"p_r", (v"e", v"f"), OUTGOING, Seq(relTypeName("R")), SimplePatternLength),
          single = true
        )(null))
      ) // 3
    )
    val optionalMatches = IndexedSeq(
      QueryGraph(patternNodes = Set("a", "a0"), argumentIds = Set("a")), // 4
      QueryGraph(patternNodes = Set("noDeps"), argumentIds = Set()), // 5
      QueryGraph(patternNodes = Set("a", "b", "c", "a1", "c1"), argumentIds = Set("a", "b", "c")), // 6
      QueryGraph(patternNodes = Set("a", "g", "a2", "g1"), argumentIds = Set("a", "g")), // 7
      QueryGraph(patternNodes = Set("a0", "a3"), argumentIds = Set("a0")), // 8
      QueryGraph(patternNodes = Set("d", "g1", "boo"), argumentIds = Set("d", "g1")), // 9
      QueryGraph(
        patternNodes = Set("c"),
        selections = Selections.from(notEquals(prop("c", "prop"), prop("r", "prop"))),
        argumentIds = Set("c", "r")
      ), // 10
      QueryGraph(
        patternNodes = Set("c"),
        selections = Selections.from(notEquals(prop("c", "prop"), Length(varFor("p"))(pos))),
        argumentIds = Set("c", "p")
      ) // 10
    )

    // WHEN
    val (gba, initialTodo) = GoalBitAllocation.create(
      components,
      QueryGraph(optionalMatches = optionalMatches)
    )

    // THEN
    gba should equal(GoalBitAllocation(
      components.size,
      optionalMatches.size,
      Seq(
        BitSet(1),
        BitSet(),
        BitSet(1, 2),
        BitSet(1, 3),
        BitSet(4),
        BitSet(2, 7),
        BitSet(1, 2),
        BitSet(2, 3)
      )
    ))
    initialTodo.take(components.size).toSet should equal(components)
    initialTodo.drop(components.size) should equal(optionalMatches)
  }

  // noinspection SameParameterValue
  private def registry(size: Int): IdRegistry[_] = {
    val r = IdRegistry.apply[Int]
    r.registerAll(0 until size)
    r
  }
}
