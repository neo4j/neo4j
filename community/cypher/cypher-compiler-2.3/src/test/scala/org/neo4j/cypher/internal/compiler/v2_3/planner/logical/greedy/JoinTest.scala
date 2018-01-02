/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.ast.PatternExpression
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class JoinTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  private def createQuery(rels: PatternRelationship*) = QueryGraph.empty.addPatternRelationships(rels)

  val aNode = IdName("a")
  val bNode = IdName("b")
  val cNode = IdName("c")
  val dNode = IdName("d")
  val r1Name = IdName("r1")
  val r2Name = IdName("r2")
  val r3Name = IdName("r3")
  val r1Rel = PatternRelationship(r1Name, (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val r2Rel = PatternRelationship(r2Name, (bNode, cNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val r3Rel = PatternRelationship(r3Name, (cNode, dNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("finds a single join") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlan(Set(aNode, bNode))
    val right = newMockedLogicalPlan(Set(bNode, cNode))
    val planTable = greedyPlanTableWith(left,right)

    val qg = createQuery(r1Rel, r2Rel)

    join(planTable, qg) should equal(Seq(
      NodeHashJoin(Set(bNode), left, right)(solved),
      NodeHashJoin(Set(bNode), right, left)(solved)
    ))
  }

  test("finds a single join that overlaps two identifiers") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlan(Set(aNode, bNode, cNode))
    val right = newMockedLogicalPlan(Set(bNode, cNode, dNode))
    val planTable = greedyPlanTableWith(left,right)

    val qg = createQuery(r1Rel, r2Rel)

    join(planTable, qg) should equal(Seq(
      NodeHashJoin(Set(bNode, cNode), left, right)(solved),
      NodeHashJoin(Set(bNode, cNode), right, left)(solved)
    ))
  }

  test("finds multiple joins") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlanWithPatterns(Set(aNode, bNode))
    val middle = newMockedLogicalPlanWithPatterns(Set(bNode, cNode))
    val right = newMockedLogicalPlanWithPatterns(Set(cNode, dNode))
    val planTable = greedyPlanTableWith(left, middle, right)

    val qg = createQuery(r1Rel, r2Rel, r3Rel)

    join(planTable, qg) should equal(Seq(
      NodeHashJoin(Set(bNode), left, middle)(solved),
      NodeHashJoin(Set(bNode), middle, left)(solved),
      NodeHashJoin(Set(cNode), middle, right)(solved),
      NodeHashJoin(Set(cNode), right, middle)(solved)
    ))
  }

  test("does not introduce joins if plans do not overlap") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlanWithPatterns(Set(aNode, bNode))
    val right = newMockedLogicalPlanWithPatterns(Set(cNode))
    val planTable = greedyPlanTableWith(left,right)

    val qg = createQuery(r1Rel)

    join(planTable, qg) shouldBe empty
  }

  test("does not join a plan with itself") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlanWithPatterns(Set(aNode))
    val planTable = greedyPlanTableWith(left)

    val qg = createQuery()

    join(planTable, qg) shouldBe empty
  }

  test("does not join relationships") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlanWithPatterns(Set(r1Name, aNode))
    val right = newMockedLogicalPlanWithPatterns(Set(r1Name, bNode))
    val planTable = greedyPlanTableWith(left,right)

    val qg = createQuery(r1Rel)
    join(planTable, qg) shouldBe empty
  }
}
