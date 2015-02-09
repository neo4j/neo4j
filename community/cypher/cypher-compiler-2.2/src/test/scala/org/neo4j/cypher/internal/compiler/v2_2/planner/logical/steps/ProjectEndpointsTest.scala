/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.InputPosition.NONE
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.graphdb.Direction

class ProjectEndpointsTest
  extends CypherFunSuite
  with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  val aName = IdName("a")
  val bName = IdName("b")
  val rName = IdName("r")

  test("project single simple outgoing relationship") {
    implicit val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext)
    val inputPlan = planArgumentRow(patternNodes = Set.empty, patternRels = Set.empty, other = Set(rName))
    val planTable = planTableWith(inputPlan)

    val patternRel = PatternRelationship(rName, (aName, bName), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph.empty.addPatternRel(patternRel)

    projectEndpoints(planTable, qg) should equal(Seq(
      planEndpointProjection(inputPlan, aName, bName, Seq.empty, patternRel)
    ))
  }

  test("project single simple outgoing relationship and verifies it's type") {
    implicit val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext)
    val inputPlan = planArgumentRow(patternNodes = Set.empty, patternRels = Set.empty, other = Set(rName))
    val planTable = planTableWith(inputPlan)

    val patternRel = PatternRelationship(rName, (aName, bName), Direction.OUTGOING, Seq(RelTypeName("X")_), SimplePatternLength)
    val qg = QueryGraph.empty.addPatternRel(patternRel)

    projectEndpoints(planTable, qg) should equal(Seq(
      planEndpointProjection(inputPlan, aName, bName, Seq(), patternRel)
    ))
  }

  test("project single simple incoming relationship") {
    implicit val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext)
    val inputPlan = planArgumentRow(patternNodes = Set.empty, patternRels = Set.empty, other = Set(rName))
    val planTable = planTableWith(inputPlan)

    val patternRel = PatternRelationship(rName, (aName, bName), Direction.INCOMING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph.empty.addPatternRel(patternRel)

    projectEndpoints(planTable, qg) should equal(Seq(
      planEndpointProjection(inputPlan, bName, aName, Seq.empty, patternRel)
    ))
  }

  test("project single simple outgoing relationship where start node is bound") {
    implicit val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext)
    val inputPlan = planArgumentRow(patternNodes = Set.empty, patternRels = Set.empty, other = Set(aName, rName))
    val planTable = planTableWith(inputPlan)

    val patternRel = PatternRelationship(rName, (aName, bName), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph.empty.addPatternRel(patternRel)

    projectEndpoints(planTable, qg) should equal(Seq(
      planEndpointProjection(inputPlan, freshName(aName), bName, Seq(freshlyEqualTo(aName)), patternRel)
    ))
  }

  test("project single simple outgoing relationship where end node is bound") {
    implicit val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext)
    val inputPlan = planArgumentRow(patternNodes = Set.empty, patternRels = Set.empty, other = Set(bName, rName))
    val planTable = planTableWith(inputPlan)

    val patternRel = PatternRelationship(rName, (aName, bName), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph.empty.addPatternRel(patternRel)

    projectEndpoints(planTable, qg) should equal(Seq(
      planEndpointProjection(inputPlan, aName, freshName(bName), Seq(freshlyEqualTo(bName)), patternRel)
    ))
  }

  test("project single simple outgoing relationship where both nodes are bound") {
    implicit val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext)
    val inputPlan = planArgumentRow(patternNodes = Set.empty, patternRels = Set.empty, other = Set(aName, bName, rName))
    val planTable = planTableWith(inputPlan)

    val patternRel = PatternRelationship(rName, (aName, bName), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph.empty.addPatternRel(patternRel)

    projectEndpoints(planTable, qg) should equal(Seq(
      planEndpointProjection(inputPlan, freshName(aName), freshName(bName), Seq(freshlyEqualTo(aName), freshlyEqualTo(bName)), patternRel)
    ))
  }

  private def freshName(node: IdName) = node.name + "$$$_"

  private def freshlyEqualTo(node: IdName): Expression = Equals(Identifier(node.name)(NONE), Identifier(freshName(node.name))(NONE))(NONE)
}
