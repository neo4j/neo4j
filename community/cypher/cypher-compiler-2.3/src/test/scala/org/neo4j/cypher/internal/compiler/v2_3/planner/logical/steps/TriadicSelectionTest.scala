/*
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.ast.{RelTypeName, Equals, Identifier, Not}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.QueryGraphProducer
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Direction

class TriadicSelectionTest extends CypherFunSuite with LogicalPlanningTestSupport with QueryGraphProducer {

  implicit val ctx = newMockedLogicalPlanningContext(mock[PlanContext])

  test("plan passes through") {
    val plan = newMockedLogicalPlan()

    triadicSelection(plan, QueryGraph.empty) should equal(Seq.empty)
  }

  test("MATCH (a:X)-->(b)-->(c) WHERE NOT (a)-->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b)-[r2]->(c) WHERE NOT (a)-->(c)")

    val lblScan = NodeByLabelScan(IdName("a"), LazyLabel("X"), Set.empty)(solved)
    val expand1 = Expand(lblScan, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val expand2 = Expand(expand1, IdName("b"), Direction.OUTGOING, Seq.empty, IdName("c"), IdName("r2"), ExpandAll)(solved)
    val selection = Selection(Seq(Not(Equals(Identifier("r1")(pos), Identifier("r2")(pos))(pos))(pos)), expand2)(solved)

    val triadicBuild = TriadicBuild(expand1, IdName("b"))(solved)
    val expand2B = Expand(triadicBuild, IdName("b"), Direction.OUTGOING, Seq.empty, IdName("c"), IdName("r2"), ExpandAll)(solved)
    val selectionB = Selection(Seq(Not(Equals(Identifier("r1")(pos), Identifier("r2")(pos))(pos))(pos)), expand2B)(solved)
    val triadicProbe = TriadicProbe(selectionB, IdName("b"), IdName("c"))(solved)

    triadicSelection(selection, plannerQuery.lastQueryGraph) should equal(Seq(triadicProbe))
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE NOT (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE NOT (a)-->(c)")

    val lblScan = NodeByLabelScan(IdName("a"), LazyLabel("X"), Set.empty)(solved)
    val expand1 = Expand(lblScan, IdName("a"), Direction.OUTGOING, Seq(RelTypeName("A")(pos)), IdName("b"), IdName("r1"), ExpandAll)(solved)
    val expand2 = Expand(expand1, IdName("b"), Direction.OUTGOING, Seq(RelTypeName("B")(pos)), IdName("c"), IdName("r2"), ExpandAll)(solved)
    val selection = Selection(Seq(Not(Equals(Identifier("r1")(pos), Identifier("r2")(pos))(pos))(pos)), expand2)(solved)

    triadicSelection(selection, plannerQuery.lastQueryGraph) should equal(Seq.empty)
  }
}
